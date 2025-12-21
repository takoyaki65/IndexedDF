package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import scala.collection.concurrent.TrieMap
import indexeddataframe.RowBatch
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeRowJoiner, UnsafeRowJoiner}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

object InternalIndexedPartition {

  /** Sentinel value indicating the end of a linked list of rows with the same key */
  final val EndOfListSentinel: Long = 0xffffffffffffffffL

  // =====================================================================================
  // Bit packing configuration for 64-bit row pointers
  // =====================================================================================
  //
  // The pointer layout divides a 64-bit integer into two fields:
  //   - Batch number: identifies which RowBatch contains the row (upper 32 bits)
  //   - Offset: byte offset within the RowBatch (lower 32 bits)
  //
  // Row size is stored inline at the beginning of each row data (4-byte header).
  //
  // Current configuration:
  //   - 32 bits for batch number → up to ~4 billion batches
  //   - 32 bits for offset → up to 4GB per batch (128MB used by default)
  //   - Row size stored as 4-byte (32-bit) header → up to ~2GB per row

  /** Number of bits to represent the batch number (upper 32 bits) */
  final val NoBitsBatches: Int = 32

  /** Number of bits to represent the offset within a batch (lower 32 bits) */
  final val NoBitsOffsets: Int = 32

  /** Size header bytes prepended to each row (stores row size as 4-byte int) */
  final val RowSizeHeaderBytes: Int = 4

  /** RowBatch size: 128MB per batch (aligned with Spark's default partition size) */
  final val BatchSize: Int = 128 * 1024 * 1024
}

/**
 * Internal data structure representing a single partition of an Indexed DataFrame.
 *
 * == Overview ==
 *
 * This class stores row data in off-heap memory (via [[RowBatch]]) and maintains a hash index
 * (via CTrie) for fast key lookups. Each partition is independent and can be processed in parallel.
 *
 * == Data Storage Architecture ==
 *
 * {{{
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                     InternalIndexedPartition                    │
 *   ├─────────────────────────────────────────────────────────────────┤
 *   │                                                                 │
 *   │   ┌─────────────────┐          ┌─────────────────────────────┐  │
 *   │   │   CTrie Index   │          │        RowBatches           │  │
 *   │   │ (key → pointer) │          │  (off-heap row storage)     │  │
 *   │   ├─────────────────┤          ├─────────────────────────────┤  │
 *   │   │ key1 → ptr1     │─────────>│ RowBatch 0 (4MB)            │  │
 *   │   │ key2 → ptr2     │          │  ├─ row1 + #prev            │  │
 *   │   │ key3 → ptr3     │          │  ├─ row2 + #prev            │  │
 *   │   │   ...           │          │  └─ ...                     │  │
 *   │   └─────────────────┘          │ RowBatch 1 (4MB)            │  │
 *   │                                │  └─ ...                     │  │
 *   │                                └─────────────────────────────┘  │
 *   └─────────────────────────────────────────────────────────────────┘
 * }}}
 *
 * == Row Pointer Encoding ==
 *
 * Each index entry maps a key to a 64-bit packed pointer containing:
 *
 * {{{
 *   64-bit pointer layout:
 *   ┌─────────────────────────┬─────────────────────────┐
 *   │    Batch No (32bit)     │     Offset (32bit)      │
 *   │   (up to ~4 billion)    │     (up to 4GB)         │
 *   └─────────────────────────┴─────────────────────────┘
 *   MSB                                              LSB
 *
 *   Row data layout in RowBatch:
 *   ┌──────────────┬─────────────────────────────────────┐
 *   │ Size (4byte) │           Row Data (N bytes)        │
 *   └──────────────┴─────────────────────────────────────┘
 *
 *   This design supports rows up to ~2GB (Integer.MAX_VALUE bytes).
 * }}}
 *
 * == Duplicate Key Handling ==
 *
 * Multiple rows with the same key are stored as a linked list using backward pointers:
 *
 * {{{
 *   Index entry for key K:
 *
 *   index[K] ──▶ [row3 | #prev] ──▶ [row2 | #prev] ──▶ [row1 | 0xFF..FF]
 *                (newest)                                (oldest, end marker)
 *
 *   Each stored row has an extra column (#prev) pointing to the previous row
 *   with the same key. The sentinel value 0xFFFFFFFFFFFFFFFF marks the end.
 * }}}
 *
 * == Thread Safety ==
 *
 * Uses CTrie (concurrent trie) for both index and rowBatches maps, providing:
 * - Lock-free reads with snapshot isolation
 * - Atomic updates for concurrent writes
 * - Efficient snapshots for copy-on-write semantics
 *
 * == Memory Management ==
 *
 * Row data is stored in off-heap memory via RowBatch:
 * - Each batch is 4MB by default
 * - New batches are created when the current one is full
 * - Off-heap storage avoids GC pressure for large datasets
 */
class InternalIndexedPartition {
  import InternalIndexedPartition._

  // =====================================================================================
  // Core data structures
  // =====================================================================================

  /**
   * The hash index: maps key (Long) to packed row pointer (Long).
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
   * Off-heap storage for row data, organized as a map of batch ID to RowBatch.
   *
   * Uses CTrie for thread-safe access and snapshot capability.
   */
  var rowBatches: TrieMap[Int, RowBatch] = null

  /** Number of RowBatches currently allocated */
  var nRowBatches = 0

  // =====================================================================================
  // Row manipulation utilities
  // =====================================================================================

  /**
   * Joiner that appends the #prev column to each row during insertion.
   *
   * Uses [[CustomUnsafeRowJoiner]] to write directly to off-heap memory,
   * avoiding intermediate allocations.
   */
  private var backwardPointerJoiner: CustomUnsafeRowJoiner = null

  /**
   * Projection for converting InternalRow to UnsafeRow if needed.
   *
   * Most rows are already UnsafeRow, but this handles edge cases.
   */
  private var convertToUnsafe: UnsafeProjection = null

  // =====================================================================================
  // Initialization and setup
  // =====================================================================================

  /**
   * Initializes the internal data structures.
   *
   * Must be called before using the partition. Creates empty CTrie maps
   * for both the index and row batches.
   */
  def initialize(): Unit = {
    index = new TrieMap[Long, Long]
    rowBatches = new TrieMap[Int, RowBatch]
  }

  /** Number of rows stored in this partition */
  var nRows: Int = 0

  /** Total size of row data in bytes (for statistics) */
  var dataSize: Long = 0

  /**
   * Creates a new RowBatch and adds it to the rowBatches map.
   *
   * Called during initialization and when the current batch is full.
   */
  private def createRowBatch(): Unit = {
    rowBatches.put(nRowBatches, new RowBatch(BatchSize))
    nRowBatches += 1
  }

  /**
   * Returns a RowBatch that can accommodate a row of the given size.
   *
   * If the current batch doesn't have enough space, a new batch is created.
   *
   * @param size The size in bytes of the row to be inserted
   * @return A RowBatch with enough space for the row
   */
  private def getBatchForRowSize(size: Int): RowBatch = {
    if (!rowBatches.get(nRowBatches - 1).get.canInsert(size)) {
      // Current batch is full, create a new one
      createRowBatch()
    }
    rowBatches.get(nRowBatches - 1).get
  }

  /**
   * Configures the partition with schema and creates the hash index on a specific column.
   *
   * This method:
   * 1. Builds the schema from the provided output attributes
   * 2. Sets up the index column
   * 3. Creates the joiner for appending #prev pointers to rows
   * 4. Allocates the first RowBatch
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
    // This column stores the backward pointer for duplicate key handling
    var rightSchema = new StructType()
    val rightField = new StructField("prev", LongType)
    rightSchema = rightSchema.add(rightField)

    // Generate code for joining rows with the #prev column
    // Uses CustomUnsafeRowJoiner to write directly to off-heap memory
    this.backwardPointerJoiner = GenerateCustomUnsafeRowJoiner.create(schema, rightSchema)

    // Initialize projection for converting to UnsafeRow if needed
    this.convertToUnsafe = UnsafeProjection.create(schema)

    // Allocate the first RowBatch
    createRowBatch()
  }

  // =====================================================================================
  // Bit packing/unpacking for row pointers
  // =====================================================================================

  /**
   * Packs batch number and offset into a single 64-bit pointer.
   *
   * Layout (from MSB to LSB):
   * {{{
   *   [batchNo: 32 bits][offset: 32 bits]
   * }}}
   *
   * Note: Row size is no longer packed into the pointer. Instead, it is stored
   * as a 4-byte header at the beginning of each row in the RowBatch.
   *
   * @param batchNo The RowBatch ID (0 to ~4 billion)
   * @param offset  Byte offset within the RowBatch (0 to ~4 billion)
   * @return A packed 64-bit pointer
   */
  private def packBatchOffset(batchNo: Int, offset: Int): Long = {
    (batchNo.toLong << NoBitsOffsets) | (offset.toLong & 0xffffffffL)
  }

  /**
   * Unpacks a 64-bit pointer into its component parts.
   *
   * @param value The packed 64-bit pointer
   * @return A tuple of (batchNo, offset)
   */
  private def unpackBatchOffset(value: Long): (Int, Int) = {
    val batchNo = (value >>> NoBitsOffsets).toInt
    val offset = value.toInt // Lower 32 bits
    (batchNo, offset)
  }

  /**
   * Reads the row size from the 4-byte header at the given memory location.
   *
   * @param baseAddress Base address of the RowBatch
   * @param offset      Offset within the RowBatch where the size header starts
   * @return The row size in bytes
   */
  private def readRowSizeHeader(baseAddress: Long, offset: Int): Int = {
    Platform.getInt(null, baseAddress + offset)
  }

  /**
   * Writes the row size to the 4-byte header at the given memory location.
   *
   * @param baseAddress Base address of the RowBatch
   * @param offset      Offset within the RowBatch where the size header starts
   * @param size        The row size in bytes
   */
  private def writeRowSizeHeader(baseAddress: Long, offset: Int, size: Int): Unit = {
    Platform.putInt(null, baseAddress + offset, size)
  }

  // =====================================================================================
  // Key normalization
  // =====================================================================================

  /**
   * Normalizes a key value to Long for index storage and lookup.
   *
   * This method handles different key types:
   * - Long: used as-is
   * - Int: converted to Long
   * - String: hashed using Murmur3 to produce a Long
   * - Double: truncated to Long
   *
   * @param key The key value (from UnsafeRow or external lookup)
   * @return The normalized Long key for index operations
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
   *
   * @param unsafeRow The row to extract the key from
   * @return The normalized Long key
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
   * This method performs the following steps:
   * 1. Extracts the key from the index column (converting to Long if needed)
   * 2. Creates a #prev row containing a placeholder for the backward pointer
   * 3. Writes the 4-byte size header, then joins the row with #prev to off-heap memory
   * 4. Updates the index, linking to any existing rows with the same key
   *
   * Memory layout for each row:
   * {{{
   *   ┌──────────────┬─────────────────────────────────────┐
   *   │ Size (4byte) │   Row Data (original + #prev col)   │
   *   └──────────────┴─────────────────────────────────────┘
   * }}}
   *
   * Duplicate key handling:
   * - If a row with the same key already exists, the new row's #prev pointer
   *   is set to point to the existing row (forming a linked list)
   * - If no existing row, #prev is set to 0xFFFFFFFFFFFFFFFF (end marker)
   *
   * @param row The row to append (InternalRow, will be converted to UnsafeRow if needed)
   */
  def appendRow(row: InternalRow): Unit = {
    // NOTE: Assume input rows are always UnsafeRow for performance
    val unsafeRow = row.asInstanceOf[UnsafeRow]

    // Update data size statistics
    this.dataSize += unsafeRow.getSizeInBytes()

    // Extract and normalize key from index column
    val key = extractKeyFromRow(unsafeRow)

    // Create a single-column UnsafeRow for the #prev pointer
    // Layout: 8 bytes null bitset + 8 bytes Long value = 16 bytes total
    val prevRow = new UnsafeRow(1)
    val prevByteArray = new Array[Byte](16)
    // Clear the null bitset (first 8 bytes)
    for (i <- 0 to 7) prevByteArray(i) = 0
    prevRow.pointTo(prevByteArray, 16)

    // Calculate total size needed: 4-byte size header + row data + 8 bytes for #prev
    val estimatedRowSize = unsafeRow.getSizeInBytes + 8 // row + #prev column
    val totalSizeNeeded = RowSizeHeaderBytes + estimatedRowSize

    // Get a batch with enough space
    val crntBatch = getBatchForRowSize(totalSizeNeeded)
    val offset = crntBatch.getCurrentOffset()
    val ptr = crntBatch.getCurrentPointer()

    // Reserve space for size header (write row data after the header)
    val rowDataOffset = offset + RowSizeHeaderBytes

    // Join the row with #prev column, writing directly to off-heap memory
    var t1 = System.nanoTime()
    val resultRow = backwardPointerJoiner.join(unsafeRow, prevRow, ptr + rowDataOffset)
    var t2 = System.nanoTime()
    totalProjections += (t2 - t1)

    // Write the size header at the beginning
    val rowSize = resultRow.getSizeInBytes
    writeRowSizeHeader(ptr, offset, rowSize)

    // Create packed pointer to this row's location (points to size header)
    val cTriePointer = packBatchOffset(nRowBatches - 1, offset)

    // Handle duplicate keys by linking to existing row
    val existingPointer = index.get(key)
    if (existingPointer.isDefined) {
      // Link to existing row (this row becomes the new head of the list)
      resultRow.setLong(this.nColumns, existingPointer.get)
    } else {
      // No existing row, mark as end of list
      resultRow.setLong(this.nColumns, EndOfListSentinel)
    }

    // Update index to point to this (newest) row
    this.index.put(key, cTriePointer)

    // Finalize the row in the batch (size header + row data)
    t1 = System.nanoTime()
    crntBatch.updateAppendedRowSize(RowSizeHeaderBytes + rowSize)
    t2 = System.nanoTime()
    totalAppend += (t2 - t1)

    this.nRows += 1
  }

  /** Accumulated time spent in row projection (nanoseconds, for profiling) */
  var totalProjections = 0.0

  /** Accumulated time spent in batch append operations (nanoseconds, for profiling) */
  var totalAppend = 0.0

  /**
   * Appends multiple rows to this partition.
   *
   * @param rows Iterator of rows to append
   */
  def appendRows(rows: Iterator[InternalRow]): Unit = {
    rows.foreach { row =>
      appendRow(row)
    }
    println("append took %f, projection took %f".format(totalAppend / 1000000.0, totalProjections / 1000000.0))
  }

  // =====================================================================================
  // Key lookup and row iteration
  // =====================================================================================

  /**
   * Iterator that traverses all rows with the same key using backward pointers.
   *
   * Follows the linked list from newest to oldest row:
   * {{{
   *   index[key] ──▶ row3 ──▶ row2 ──▶ row1 ──▶ END (0xFF..FF)
   * }}}
   *
   * Memory layout for each row:
   * {{{
   *   ┌──────────────┬─────────────────────────────────────┐
   *   │ Size (4byte) │   Row Data (original + #prev col)   │
   *   └──────────────┴─────────────────────────────────────┘
   *   ^               ^
   *   offset          offset + 4 (row data starts here)
   * }}}
   *
   * @param rowPointer Initial packed pointer from the index
   */
  class RowIterator(rowPointer: Long) extends Iterator[InternalRow] {
    // Reusable UnsafeRow for returning results (includes #prev column)
    private val currentRow = new UnsafeRow(schema.size + 1)
    // Current position in the linked list
    private var crntRowPointer = rowPointer

    def hasNext: Boolean = {
      // 0xFFFFFFFFFFFFFFFF is the end-of-list sentinel
      crntRowPointer != EndOfListSentinel
    }

    def next(): InternalRow = {
      // Unpack pointer to get batch number and offset
      val (batchNo, offset) = unpackBatchOffset(crntRowPointer)

      // Retrieve the RowBatch
      val batchOpt = rowBatches.get(batchNo)
      if (batchOpt.isEmpty) {
        throw new IllegalStateException(
          s"RowBatch $batchNo not found. crntRowPointer=0x${crntRowPointer.toHexString}, " +
            s"nRowBatches=$nRowBatches, rowBatches.size=${rowBatches.size}, " +
            s"rowBatches.keys=${rowBatches.keys.toSeq.sorted.mkString(",")}, " +
            s"index.size=${index.size}, nRows=$nRows"
        )
      }

      val baseAddress = batchOpt.get.rowData

      // Read the row size from the 4-byte header
      val size = readRowSizeHeader(baseAddress, offset)

      // Point to the row data (after the size header)
      val rowDataOffset = offset + RowSizeHeaderBytes
      currentRow.pointTo(null, baseAddress + rowDataOffset, size)

      // Follow the backward pointer to the next row with the same key
      crntRowPointer = currentRow.getLong(nColumns)

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
    // Normalize key to Long for index lookup
    val internalKey = normalizeKey(key)

    val rowPointer = index.get(internalKey)
    if (rowPointer.isDefined) {
      new RowIterator(rowPointer.get)
    } else {
      // Return empty iterator (end marker only)
      new RowIterator(EndOfListSentinel)
    }
  }

  // =====================================================================================
  // Full partition iteration (for scans)
  // =====================================================================================

  /**
   * Iterator that traverses ALL rows in the partition.
   *
   * Iteration order:
   * 1. Iterate over all unique keys in the index
   * 2. For each key, iterate over all rows with that key (following backward pointers)
   *
   * This is used for full table scans and RDD operations.
   *
   * Implementation uses flatMap to chain all row iterators together,
   * which is idiomatic Scala and handles empty partitions naturally.
   */
  class PartitionIterator() extends Iterator[InternalRow] {
    // Chain all row iterators: for each key, get all rows with that key
    private val underlying: Iterator[InternalRow] =
      index.keysIterator.flatMap(key => get(key))

    override def knownSize: Int = nRows

    def hasNext: Boolean = underlying.hasNext

    def next(): InternalRow = underlying.next().copy()
  }

  /**
   * Returns an iterator over all rows in this partition.
   *
   * Used by the RDD interface for full table scans.
   *
   * @return Iterator over all rows
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
   *
   * @param keys Array of keys to look up
   * @return Iterator over all matching rows (copied)
   */
  def multiget(keys: Array[AnyVal]): Iterator[InternalRow] = {
    keys.iterator.flatMap { key =>
      get(key).map(_.copy())
    }
  }

  /**
   * Performs an indexed join where this partition is the LEFT (indexed) side.
   *
   * For each row in rightIter:
   * 1. Extract the join key from the right row
   * 2. Look up matching rows in this partition's index
   * 3. Join each matching left row with the right row
   *
   * @param rightIter    Iterator of rows from the right (non-indexed) side
   * @param joiner       UnsafeRowJoiner to combine left and right rows
   * @param rightOutput  Output attributes for the right side (for type info)
   * @param joinRightCol Column index in right row containing the join key
   * @return Iterator of joined rows
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
   *
   * For each row in leftIter:
   * 1. Extract the join key from the left row
   * 2. Look up matching rows in this partition's index
   * 3. Join the left row with each matching right row
   *
   * @param leftIter    Iterator of rows from the left (non-indexed) side
   * @param joiner      UnsafeRowJoiner to combine left and right rows
   * @param leftOutput  Output attributes for the left side (for type info)
   * @param joinLeftCol Column index in left row containing the join key
   * @return Iterator of joined rows
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
   * This enables the `appendRows` operation on IndexedDataFrame without modifying
   * the original cached data. The snapshot shares existing data with the original
   * but can accept new rows independently.
   *
   * How it works:
   * - CTrie's snapshot() creates a read-only view of the existing data
   * - A new RowBatch is allocated for the copy to receive new writes
   * - The original partition is unaffected by writes to the copy
   *
   * Performance:
   * - O(1) operation (no data copying)
   * - CTrie handles all the complexity of concurrent snapshot isolation
   *
   * {{{
   *   Original:                      Snapshot (copy):
   *   ┌──────────────┐              ┌──────────────┐
   *   │ index        │◀─ shared ──▶│ index.snap() │
   *   │ rowBatches   │◀─ shared ──▶│ rowBatches   │
   *   │ batch[N]     │              │ batch[N+1]   │ ← new writes go here
   *   └──────────────┘              └──────────────┘
   * }}}
   *
   * @return A new partition that shares existing data but can be written to independently
   */
  def getSnapshot(): InternalIndexedPartition = {
    val copy = new InternalIndexedPartition

    // Create read-only snapshots of both CTries
    copy.index = this.index.snapshot()
    copy.rowBatches = this.rowBatches.snapshot()

    // Allocate a new RowBatch for the copy to avoid write conflicts
    // The original's current batch might still be written to
    copy.nRowBatches = this.nRowBatches
    copy.createRowBatch()

    // Copy metadata
    copy.schema = this.schema
    copy.indexCol = this.indexCol
    copy.nColumns = this.nColumns
    copy.nRows = this.nRows

    // Initialize projections for the copy
    var rightSchema = new StructType()
    val rightField = new StructField("prev", LongType)
    rightSchema = rightSchema.add(rightField)
    copy.backwardPointerJoiner = GenerateCustomUnsafeRowJoiner.create(schema, rightSchema)
    copy.convertToUnsafe = UnsafeProjection.create(schema)

    copy
  }
}
