package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import scala.collection.concurrent.TrieMap
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import scala.collection.mutable.ArrayBuffer

object InternalIndexedPartition {

  /** Sentinel value indicating the end of a linked list of rows with the same key */
  final val EndOfListSentinel: Long = 0xffffffffffffffffL

  /** Size header bytes prepended to each row (stores row size as 4-byte int) */
  final val RowSizeHeaderBytes: Int = 4

  /** Prev pointer size in bytes */
  final val PrevPointerBytes: Int = 8

  /** Number of bits used for page index in encoded pointer */
  final val PageIndexBits: Int = 16

  /** Number of bits used for offset in encoded pointer */
  final val OffsetBits: Int = 48

  /** Maximum page index value */
  final val MaxPageIndex: Int = (1 << PageIndexBits) - 1

  /** Maximum offset value */
  final val MaxOffset: Long = (1L << OffsetBits) - 1

  /** Mask for extracting offset from encoded pointer */
  final val OffsetMask: Long = MaxOffset

  /** Default page size: 4MB */
  final val DefaultPageSize: Int = 4 * 1024 * 1024

  /**
   * Encodes page index and offset into a single Long pointer.
   *
   * Layout: [16-bit pageIndex][48-bit offset]
   */
  def encodePointer(pageIndex: Int, offset: Long): Long = {
    (pageIndex.toLong << OffsetBits) | (offset & OffsetMask)
  }

  /**
   * Decodes page index from an encoded pointer.
   */
  def decodePageIndex(pointer: Long): Int = {
    (pointer >>> OffsetBits).toInt
  }

  /**
   * Decodes offset from an encoded pointer.
   */
  def decodeOffset(pointer: Long): Long = {
    pointer & OffsetMask
  }
}

/**
 * Internal data structure representing a single partition of an Indexed DataFrame.
 *
 * == Overview ==
 *
 * This class stores row data in on-heap byte arrays (pages) and maintains
 * a hash index (via CTrie) for fast key lookups. Each partition is independent and can
 * be processed in parallel.
 *
 * == Memory Management ==
 *
 * Row data is stored in on-heap byte arrays:
 * - Memory is allocated in pages (default 4MB byte arrays)
 * - During indexing, rows are accumulated in a buffer
 * - When buffer exceeds page size, it's finalized as an immutable page
 * - finishIndexing() finalizes remaining buffer with minimal memory allocation
 *
 * Memory lifecycle is managed by Spark's RDD cache mechanism:
 * - When RDD[InternalIndexedPartition].cache() is called, Spark serializes
 *   the partition (including pages) and stores it in BlockManager
 * - When unpersist() is called, Spark removes the blocks and the objects
 *   become eligible for garbage collection
 *
 * == Data Storage Architecture ==
 *
 * {{{
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                     InternalIndexedPartition                    │
 *   ├─────────────────────────────────────────────────────────────────┤
 *   │                                                                 │
 *   │   ┌─────────────────┐          ┌─────────────────────────────┐  │
 *   │   │   CTrie Index   │          │     Pages (immutable)       │  │
 *   │   │ (key → pointer) │          │ ArrayBuffer[Array[Byte]]    │  │
 *   │   ├─────────────────┤          ├─────────────────────────────┤  │
 *   │   │ key1 → ptr1     │─────────>│ Page 0 (byte[])             │  │
 *   │   │ key2 → ptr2     │          │  ├─ row1 + #prev            │  │
 *   │   │ key3 → ptr3     │          │  ├─ row2 + #prev            │  │
 *   │   │   ...           │          │  └─ ...                     │  │
 *   │   └─────────────────┘          │ Page 1 (byte[])             │  │
 *   │                                │  └─ ...                     │  │
 *   │                                └─────────────────────────────┘  │
 *   └─────────────────────────────────────────────────────────────────┘
 * }}}
 *
 * == Pointer Format ==
 *
 * Index entries store encoded 64-bit pointers:
 * {{{
 *   [16-bit pageIndex][48-bit offset]
 *
 *   - pageIndex: Index into the pages array (supports up to 65535 pages)
 *   - offset: Byte offset within the page (supports up to 256TB per page)
 * }}}
 *
 * == Row Data Layout ==
 *
 * Each row in memory:
 * {{{
 *   ┌──────────────┬─────────────────────────┬──────────────┐
 *   │ Size (4byte) │     UnsafeRow Data      │ Prev (8byte) │
 *   └──────────────┴─────────────────────────┴──────────────┘
 * }}}
 *
 * == Duplicate Key Handling ==
 *
 * Multiple rows with the same key form a linked list via prev pointers:
 * {{{
 *   index[K] ──▶ [row3 | prev] ──▶ [row2 | prev] ──▶ [row1 | 0xFF..FF]
 *                (newest)                              (oldest, end marker)
 * }}}
 *
 * == Thread Safety ==
 *
 * Uses CTrie (concurrent trie) for the index, providing:
 * - Lock-free reads with snapshot isolation
 * - Atomic updates for concurrent writes
 */
class InternalIndexedPartition extends Serializable {
  import InternalIndexedPartition._

  // =====================================================================================
  // Core data structures
  // =====================================================================================

  /**
   * The hash index: maps key (Long) to row address (Long).
   *
   * Uses CTrie for thread-safe concurrent access.
   * Non-Long keys (String, Int, Double) are converted to Long via hashing.
   */
  var index: TrieMap[Long, Long] = null

  /** Schema of the stored rows (original columns, not including #prev) */
  private var schema: StructType = null

  /** Column index on which the hash index is built (0-based) */
  private var indexCol: Int = 0

  /** Number of columns in the original schema (not including #prev) */
  private var nColumns: Int = 0

  /**
   * Immutable pages containing finalized row data.
   */
  @transient private var pages: ArrayBuffer[Array[Byte]] = null

  /**
   * Buffer for accumulating rows before finalizing into a page.
   * Size is DefaultPageSize. When buffer fills up, it becomes a page.
   */
  @transient private var buffer: Array[Byte] = null

  /** Current write offset within the buffer */
  @transient private var bufferOffset: Int = 0

  /** RDD ID for this partition (used for block identification) */
  private var rddId: Int = 0

  /** Partition ID within the RDD */
  private var partitionId: Int = 0

  /** Total memory used by pages (for statistics) */
  @transient private var totalMemoryUsed: Long = 0

  // =====================================================================================
  // Row manipulation utilities
  // =====================================================================================

  /**
   * Joiner that appends the #prev column to each row during insertion.
   *
   * Uses [[CustomUnsafeRowJoiner]] to write directly to memory,
   * avoiding intermediate allocations.
   */
  @transient private var backwardPointerJoiner: CustomUnsafeRowJoiner = null

  /**
   * Projection for converting InternalRow to UnsafeRow if needed.
   */
  @transient private var convertToUnsafe: UnsafeProjection = null

  // =====================================================================================
  // Initialization and setup
  // =====================================================================================

  /**
   * Initializes the internal data structures.
   *
   * @param rddId       The RDD ID for memory block identification
   * @param partitionId The partition ID within the RDD
   */
  def initialize(rddId: Int, partitionId: Int): Unit = {
    this.rddId = rddId
    this.partitionId = partitionId
    index = new TrieMap[Long, Long]
    pages = new ArrayBuffer[Array[Byte]]()
    buffer = null
    bufferOffset = 0
    totalMemoryUsed = 0
  }

  /** Number of rows stored in this partition */
  var nRows: Int = 0

  /** Total size of row data in bytes (for statistics) */
  var dataSize: Long = 0

  /**
   * Configures the partition with schema and creates the hash index on a specific column.
   *
   * @param output   Sequence of output attributes (defines schema and column metadata)
   * @param columnNo 0-based index of the column to build the hash index on
   */
  def createIndex(output: Seq[Attribute], columnNo: Int): Unit = {
    // Build schema from output attributes
    this.schema = new StructType()
    for (attr <- output) {
      val field = new StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
      this.schema = this.schema.add(field)
    }
    this.nColumns = output.length
    this.indexCol = columnNo

    // Create a schema for the #prev column (single LongType field)
    var rightSchema = new StructType()
    val rightField = new StructField("prev", LongType)
    rightSchema = rightSchema.add(rightField)

    // Generate code for joining rows with the #prev column
    this.backwardPointerJoiner = GenerateCustomUnsafeRowJoiner.create(schema, rightSchema)

    // Initialize projection for converting to UnsafeRow if needed
    this.convertToUnsafe = UnsafeProjection.create(schema)
  }

  // =====================================================================================
  // Key normalization
  // =====================================================================================

  /**
   * Normalizes a key value to Long for index storage and lookup.
   */
  private def normalizeKey(key: Any): Long = {
    key match {
      case l: Long => l
      case i: Int  => i.toLong
      case s: String =>
        Murmur3_x86_32.hashUnsafeBytes(
          s.getBytes(),
          Platform.BYTE_ARRAY_OFFSET,
          s.length,
          42 // seed
        )
      case d: Double => d.toLong
      case _         => key.asInstanceOf[Long] // fallback
    }
  }

  /**
   * Extracts and normalizes the key from an UnsafeRow based on the index column type.
   */
  private def extractKeyFromRow(unsafeRow: UnsafeRow): Long = {
    schema(indexCol).dataType match {
      case LongType    => unsafeRow.getLong(indexCol)
      case IntegerType => unsafeRow.getInt(indexCol).toLong
      case StringType =>
        val str = unsafeRow.getString(indexCol)
        Murmur3_x86_32.hashUnsafeBytes(
          str.getBytes(),
          Platform.BYTE_ARRAY_OFFSET,
          str.length,
          42 // seed
        )
      case DoubleType => unsafeRow.getDouble(indexCol).toLong
      case _          => unsafeRow.getLong(indexCol) // fallback
    }
  }

  // =====================================================================================
  // Buffer and page management
  // =====================================================================================

  /**
   * Ensures a buffer is available for writing. Allocates if needed.
   */
  private def ensureBuffer(): Unit = {
    if (buffer == null) {
      buffer = new Array[Byte](DefaultPageSize)
      bufferOffset = 0
    }
  }

  /**
   * Finalizes the current buffer as an immutable page.
   * The buffer is added to pages and a new buffer will be allocated on next write.
   */
  private def finalizeBuffer(): Unit = {
    if (buffer != null && bufferOffset > 0) {
      // Create a right-sized array for this page
      val page = new Array[Byte](bufferOffset)
      System.arraycopy(buffer, 0, page, 0, bufferOffset)
      pages += page
      totalMemoryUsed += bufferOffset

      buffer = null
      bufferOffset = 0
    }
  }

  // =====================================================================================
  // Row insertion methods
  // =====================================================================================

  /**
   * Appends a single row to this partition.
   *
   * Memory layout for each row:
   * {{{
   *   ┌──────────────┬─────────────────────────┬──────────────┐
   *   │ Size (4byte) │     UnsafeRow Data      │ Prev (8byte) │
   *   └──────────────┴─────────────────────────┴──────────────┘
   * }}}
   *
   * @param row The row to append (InternalRow, will be converted to UnsafeRow if needed)
   */
  def appendRow(row: InternalRow): Unit = {
    val unsafeRow = row.asInstanceOf[UnsafeRow]

    // Update data size statistics
    this.dataSize += unsafeRow.getSizeInBytes

    // Extract and normalize key from index column
    val key = extractKeyFromRow(unsafeRow)

    // Create a single-column UnsafeRow for the #prev pointer
    val prevRow = new UnsafeRow(1)
    val prevByteArray = new Array[Byte](16)
    for (i <- 0 to 7) prevByteArray(i) = 0
    prevRow.pointTo(prevByteArray, 16)

    // Calculate total size needed: 4-byte size header + row data + 8 bytes for #prev
    val rowDataSize = unsafeRow.getSizeInBytes + PrevPointerBytes
    val totalSizeNeeded = RowSizeHeaderBytes + rowDataSize

    // Handle oversized rows: if row is larger than page size, create dedicated page
    if (totalSizeNeeded > DefaultPageSize) {
      // Finalize current buffer first
      finalizeBuffer()

      // Create a dedicated page for this large row
      val largePage = new Array[Byte](totalSizeNeeded)

      // Write size header
      Platform.putInt(largePage, Platform.BYTE_ARRAY_OFFSET, rowDataSize)

      // Join the row with #prev column
      val rowDataOffset = Platform.BYTE_ARRAY_OFFSET + RowSizeHeaderBytes
      val resultRow = backwardPointerJoiner.join(unsafeRow, prevRow, largePage, rowDataOffset)

      // Encode pointer for this row
      val encodedPointer = encodePointer(pages.size, 0)

      // Handle duplicate keys
      val existingPointer = index.get(key)
      if (existingPointer.isDefined) {
        resultRow.setLong(this.nColumns, existingPointer.get)
      } else {
        resultRow.setLong(this.nColumns, EndOfListSentinel)
      }

      // Add large page and update index
      pages += largePage
      totalMemoryUsed += totalSizeNeeded
      this.index.put(key, encodedPointer)
      this.nRows += 1
      return
    }

    // Ensure buffer is available
    ensureBuffer()

    // Check if row fits in current buffer, if not finalize and start new buffer
    if (bufferOffset + totalSizeNeeded > DefaultPageSize) {
      finalizeBuffer()
      ensureBuffer()
    }

    val pageIndex = pages.size // Current page index (buffer will become this page)
    val offsetInPage = bufferOffset

    // Write size header
    Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offsetInPage, rowDataSize)

    // Join the row with #prev column, writing directly to buffer
    val rowDataOffset = Platform.BYTE_ARRAY_OFFSET + offsetInPage + RowSizeHeaderBytes
    val resultRow = backwardPointerJoiner.join(unsafeRow, prevRow, buffer, rowDataOffset)

    // Encode the pointer for this row
    val encodedPointer = encodePointer(pageIndex, offsetInPage)

    // Handle duplicate keys by linking to existing row
    val existingPointer = index.get(key)
    if (existingPointer.isDefined) {
      // Link to existing row (this row becomes the new head of the list)
      resultRow.setLong(this.nColumns, existingPointer.get)
    } else {
      // No existing row, mark as end of list
      resultRow.setLong(this.nColumns, EndOfListSentinel)
    }

    // Update index to point to this (newest) row's encoded pointer
    this.index.put(key, encodedPointer)

    bufferOffset += totalSizeNeeded
    this.nRows += 1
  }

  /**
   * Appends multiple rows to this partition.
   *
   * @param rows Iterator of rows to append
   */
  def appendRows(rows: Iterator[InternalRow]): Unit = {
    rows.foreach { row =>
      appendRow(row)
    }
  }

  /**
   * Finalizes the indexing process.
   *
   * This method MUST be called after all rows have been appended.
   * It finalizes any remaining data in the buffer as a page with minimal
   * memory allocation (only the exact size needed, not full 4MB).
   */
  def finishIndexing(): Unit = {
    if (buffer != null && bufferOffset > 0) {
      // Create a right-sized array (not full 4MB)
      val page = new Array[Byte](bufferOffset)
      System.arraycopy(buffer, 0, page, 0, bufferOffset)
      pages += page
      totalMemoryUsed += bufferOffset

      buffer = null
      bufferOffset = 0
    }
  }

  // =====================================================================================
  // Key lookup and row iteration
  // =====================================================================================

  /**
   * Gets a page by index.
   */
  def getPage(pageIndex: Int): Array[Byte] = {
    pages(pageIndex)
  }

  /**
   * Iterator that traverses all rows with the same key using backward pointers.
   *
   * @param startPointer Initial encoded pointer from the index
   */
  class AddressRowIterator(startPointer: Long) extends Iterator[InternalRow] {
    // Reusable UnsafeRow for returning results (includes #prev column)
    private val currentRow = new UnsafeRow(schema.size + 1)
    // Current encoded pointer in the linked list
    private var currentPointer = startPointer

    def hasNext: Boolean = {
      currentPointer != EndOfListSentinel
    }

    def next(): InternalRow = {
      // Decode the pointer to get page index and offset
      val pageIndex = decodePageIndex(currentPointer)
      val offsetInPage = decodeOffset(currentPointer)
      val page = getPage(pageIndex)

      // Read the row size from the 4-byte header
      val size = Platform.getInt(page, Platform.BYTE_ARRAY_OFFSET + offsetInPage)

      // Point to the row data (after the size header)
      val rowDataOffset = Platform.BYTE_ARRAY_OFFSET + offsetInPage + RowSizeHeaderBytes
      currentRow.pointTo(page, rowDataOffset, size)

      // Follow the backward pointer to the next row with the same key
      currentPointer = currentRow.getLong(nColumns)

      currentRow
    }
  }

  /**
   * Looks up all rows with the given key.
   *
   * @param key The key to search for (supports Long, Int, String, Double)
   * @return Iterator over matching rows (empty iterator if key not found)
   */
  def get(key: Any): Iterator[InternalRow] = {
    val internalKey = normalizeKey(key)

    index.get(internalKey) match {
      case Some(address) => new AddressRowIterator(address)
      case None          => new AddressRowIterator(EndOfListSentinel)
    }
  }

  // =====================================================================================
  // Full partition iteration (for scans)
  // =====================================================================================

  /**
   * Iterator that traverses ALL rows in the partition.
   */
  class PartitionIterator() extends Iterator[InternalRow] {
    private val underlying: Iterator[InternalRow] =
      index.keysIterator.flatMap(key => get(key))

    override def knownSize: Int = nRows

    def hasNext: Boolean = underlying.hasNext

    def next(): InternalRow = underlying.next().copy()
  }

  /**
   * Returns an iterator over all rows in this partition.
   */
  def iterator(): Iterator[InternalRow] = {
    new PartitionIterator
  }

  /**
   * Returns the number of rows in this partition.
   */
  def size: Int = nRows

  // =====================================================================================
  // Multi-key lookup and join operations
  // =====================================================================================

  /**
   * Looks up multiple keys and returns all matching rows.
   */
  def multiget(keys: Array[AnyVal]): Iterator[InternalRow] = {
    keys.iterator.flatMap { key =>
      get(key).map(_.copy())
    }
  }

  /**
   * Performs an indexed join where this partition is the LEFT (indexed) side.
   */
  def multigetJoinedRight(
      rightIter: Iterator[InternalRow],
      joiner: UnsafeRowJoiner,
      rightOutput: Seq[Attribute],
      joinRightCol: Int
  ): Iterator[InternalRow] = {
    rightIter.flatMap { rightRow =>
      val rightKey = rightRow.get(joinRightCol, schema(indexCol).dataType)
      get(rightKey.asInstanceOf[AnyVal]).map { leftRow =>
        joiner.join(leftRow.asInstanceOf[UnsafeRow], rightRow.asInstanceOf[UnsafeRow])
      }
    }
  }

  /**
   * Performs an indexed join where this partition is the RIGHT (indexed) side.
   */
  def multigetJoinedLeft(
      leftIter: Iterator[InternalRow],
      joiner: UnsafeRowJoiner,
      leftOutput: Seq[Attribute],
      joinLeftCol: Int
  ): Iterator[InternalRow] = {
    leftIter.flatMap { leftRow =>
      val leftKey = leftRow.get(joinLeftCol, schema(indexCol).dataType)
      get(leftKey.asInstanceOf[AnyVal]).map { rightRow =>
        joiner.join(leftRow.asInstanceOf[UnsafeRow], rightRow.asInstanceOf[UnsafeRow])
      }
    }
  }

  // =====================================================================================
  // Memory management
  // =====================================================================================

  /**
   * Clears all data structures.
   * Note: Actual memory is managed by Spark's RDD cache mechanism.
   * When the RDD is unpersisted, the serialized InternalIndexedPartition
   * objects (including pages) are garbage collected.
   */
  def free(): Unit = {
    if (pages != null) {
      pages.clear()
    }
    buffer = null
    bufferOffset = 0
    totalMemoryUsed = 0
  }

  /**
   * Returns the total memory used by this partition.
   */
  def getMemoryUsed: Long = totalMemoryUsed

  /**
   * Returns the number of pages allocated.
   */
  def getPageCount: Int = {
    if (pages != null) pages.size else 0
  }
}
