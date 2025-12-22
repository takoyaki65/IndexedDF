package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import scala.collection.concurrent.TrieMap
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

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
 * This class stores row data in on-heap byte arrays (via [[PagedRowStorage]]) and maintains
 * a hash index (via CTrie) for fast key lookups. Each partition is independent and can
 * be processed in parallel.
 *
 * == Memory Management ==
 *
 * Row data is stored in on-heap byte arrays managed through Spark's Storage Memory:
 * - Memory is allocated in pages (default 4MB byte arrays) via [[PagedRowStorage]]
 * - Spark tracks memory usage and enforces limits
 * - Pages are lazily allocated (no allocation until first row is appended)
 * - Uses Spark's default on-heap mode (no special configuration required)
 *
 * == Data Storage Architecture ==
 *
 * {{{
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                     InternalIndexedPartition                    │
 *   ├─────────────────────────────────────────────────────────────────┤
 *   │                                                                 │
 *   │   ┌─────────────────┐          ┌─────────────────────────────┐  │
 *   │   │   CTrie Index   │          │     PagedRowStorage         │  │
 *   │   │ (key → pointer) │          │ (Spark Storage Memory)      │  │
 *   │   ├─────────────────┤          ├─────────────────────────────┤  │
 *   │   │ key1 → ptr1     │─────────>│ Page 0 (byte[4MB])          │  │
 *   │   │ key2 → ptr2     │          │  ├─ row1 + #prev            │  │
 *   │   │ key3 → ptr3     │          │  ├─ row2 + #prev            │  │
 *   │   │   ...           │          │  └─ ...                     │  │
 *   │   └─────────────────┘          │ Page 1 (byte[4MB])          │  │
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
 * This allows addressing rows within on-heap byte arrays.
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
 * - Efficient snapshots for copy-on-write semantics
 */
class InternalIndexedPartition extends Serializable {
  import InternalIndexedPartition._

  // =====================================================================================
  // Core data structures
  // =====================================================================================

  /**
   * The hash index: maps key (Long) to row address (Long).
   *
   * Uses CTrie for thread-safe concurrent access and efficient snapshots.
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
   * Off-heap storage for row data, managed through Spark's Storage Memory.
   */
  @transient private var storage: PagedRowStorage = null

  /** RDD ID for this partition (used for block identification) */
  private var rddId: Int = 0

  /** Partition ID within the RDD */
  private var partitionId: Int = 0

  // =====================================================================================
  // Row manipulation utilities
  // =====================================================================================

  /**
   * Joiner that appends the #prev column to each row during insertion.
   *
   * Uses [[CustomUnsafeRowJoiner]] to write directly to off-heap memory,
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
    storage = new PagedRowStorage(rddId, partitionId)
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

    // Allocate memory for this row (returns page index and offset)
    val (pageIndex, offsetInPage) = storage.allocateForRow(totalSizeNeeded)
    val page = storage.getPage(pageIndex)

    // Write size header
    Platform.putInt(page, Platform.BYTE_ARRAY_OFFSET + offsetInPage, rowDataSize)

    // Join the row with #prev column, writing directly to byte array memory
    val rowDataOffset = Platform.BYTE_ARRAY_OFFSET + offsetInPage + RowSizeHeaderBytes
    val resultRow = backwardPointerJoiner.join(unsafeRow, prevRow, page, rowDataOffset)

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

  // =====================================================================================
  // Key lookup and row iteration
  // =====================================================================================

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
      val page = storage.getPage(pageIndex)

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
  // Copy-on-Write (Snapshot) support
  // =====================================================================================

  /**
   * Creates a snapshot of this partition for copy-on-write semantics.
   *
   * The snapshot shares existing pages with the original but allocates
   * new pages for any new writes.
   *
   * @return A new partition that shares existing data but can be written to independently
   */
  def getSnapshot(): InternalIndexedPartition = {
    val copy = new InternalIndexedPartition

    // Create read-only snapshot of index
    copy.index = this.index.snapshot()

    // Create snapshot of storage (shares existing pages)
    copy.storage = if (this.storage != null) this.storage.snapshot() else null

    // Copy metadata
    copy.rddId = this.rddId
    copy.partitionId = this.partitionId
    copy.schema = this.schema
    copy.indexCol = this.indexCol
    copy.nColumns = this.nColumns
    copy.nRows = this.nRows
    copy.dataSize = this.dataSize

    // Initialize projections for the copy
    if (schema != null) {
      var rightSchema = new StructType()
      val rightField = new StructField("prev", LongType)
      rightSchema = rightSchema.add(rightField)
      copy.backwardPointerJoiner = GenerateCustomUnsafeRowJoiner.create(schema, rightSchema)
      copy.convertToUnsafe = UnsafeProjection.create(schema)
    }

    copy
  }

  /**
   * Frees all allocated memory.
   * Should be called when the partition is no longer needed.
   */
  def free(): Unit = {
    if (storage != null) {
      storage.free()
      storage = null
    }
  }

  /**
   * Returns the total memory used by this partition.
   */
  def getMemoryUsed: Long = {
    if (storage != null) storage.getMemoryUsed else 0L
  }
}
