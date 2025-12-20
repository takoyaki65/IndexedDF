package indexeddataframe;

import org.apache.spark.unsafe.Platform;

/**
 * RowBatch - Off-heap memory storage for UnsafeRow data.
 *
 * <h2>Overview</h2>
 * RowBatch is a container that stores multiple UnsafeRow byte representations
 * in a contiguous off-heap memory region. It is used by InternalIndexedPartition to
 * store the actual row data, while the index (TrieMap) stores pointers to
 * rows within these batches.
 *
 * <h2>Memory Layout</h2>
 * <pre>
 * Off-heap memory block (default 4MB):
 * +------------------+------------------+------------------+-----+
 * |      Row 0       |      Row 1       |      Row 2       | ... |
 * | (UnsafeRow bytes)| (UnsafeRow bytes)| (UnsafeRow bytes)|     |
 * +------------------+------------------+------------------+-----+
 * ^                  ^                  ^
 * offset=0           offset=row0.size   offset=row0.size+row1.size
 * </pre>
 *
 * <h2>Why Off-Heap Memory?</h2>
 * <ul>
 *   <li>Avoids GC overhead for large datasets</li>
 *   <li>Provides stable performance under memory pressure</li>
 *   <li>Allows direct memory access via Platform API</li>
 *   <li>Memory is managed manually (must be freed explicitly)</li>
 * </ul>
 *
 * <h2>Usage Pattern</h2>
 * RowBatch is typically used with CustomUnsafeRowJoiner which writes
 * directly to the off-heap memory at a specific address. The flow is:
 * <ol>
 *   <li>Get current offset via {@link #getCurrentOffset()}</li>
 *   <li>Get memory pointer via {@link #getCurrentPointer()}</li>
 *   <li>CustomUnsafeRowJoiner writes row data to (pointer + offset)</li>
 *   <li>Call {@link #updateAppendedRowSize(int)} to advance the offset</li>
 * </ol>
 *
 * <h2>Important Notes</h2>
 * <ul>
 *   <li>This class is NOT thread-safe</li>
 *   <li>Memory is NOT automatically freed - caller must manage lifecycle</li>
 *   <li>The rowData field is a raw memory address (long), not a Java object</li>
 *   <li>Serialization will NOT preserve the actual memory contents</li>
 * </ul>
 *
 * @see InternalIndexedPartition
 * @see CustomUnsafeRowJoiner
 */
class RowBatch implements AutoCloseable {

    /**
     * Flag to track whether the memory has been freed.
     * Prevents double-free errors.
     */
    private boolean freed = false;

    /**
     * Default batch size: 4MB.
     * This size is chosen to balance between:
     * - Memory efficiency (not too many small allocations)
     * - Flexibility (not too large for small datasets)
     */
    private static final int DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;

    /**
     * The allocated size of this batch in bytes.
     */
    private int batchSize;

    /**
     * Pointer to the off-heap memory block.
     * This is a raw memory address obtained from Platform.allocateMemory().
     *
     * IMPORTANT: This is NOT a Java object reference, but a native memory address.
     * When this object is serialized, only this address value is preserved,
     * NOT the actual memory contents. This means:
     * - The batch must be cached (persisted) properly in Spark
     * - If evicted and re-computed, the data will be lost
     */
    public long rowData = -1;

    /**
     * Current write position (next available offset) in bytes.
     * Also represents the total size of data written so far.
     */
    private int size = 0;

    /**
     * Offset of the last appended row.
     * Used for operations that need to reference the most recently added row.
     */
    private int lastOffset = 0;

    /**
     * Creates a new RowBatch with the specified size.
     *
     * @param batchSize The size of the off-heap memory block to allocate in bytes.
     *                  Typically 4MB (4 * 1024 * 1024).
     *
     * @throws OutOfMemoryError if the off-heap memory cannot be allocated
     */
    public RowBatch(int batchSize) {
        this.batchSize = batchSize;
        // Allocate off-heap memory using Spark's Platform API
        rowData = Platform.allocateMemory(batchSize);
        // Zero-initialize the memory block
        Platform.setMemory(rowData, (byte)0, batchSize);
    }

    /**
     * Appends a row (as byte array) to this batch.
     *
     * This method copies the row data from a Java byte array to off-heap memory.
     * Use this when you have the row data in a byte array.
     *
     * @param crntRow The row data as a byte array (typically from UnsafeRow.getBytes())
     * @return The offset within this batch where the row was stored.
     *         This offset can be used later to retrieve the row.
     *
     * @throws IndexOutOfBoundsException if the row doesn't fit in remaining space
     *         (caller should check with {@link #canInsert(int)} first)
     */
    public int appendRow(byte[] crntRow) {
        // Copy from Java heap (byte array) to off-heap memory
        Platform.copyMemory(crntRow, Platform.BYTE_ARRAY_OFFSET, null, rowData + size, crntRow.length);

        int returnedOffset = size;
        lastOffset = size;
        size += crntRow.length;
        return returnedOffset;
    }

    /**
     * Updates the size after a row has been written directly to off-heap memory.
     *
     * This method is used when the row data is written directly to the off-heap
     * memory by CustomUnsafeRowJoiner (which writes at rowData + getCurrentOffset()).
     * After the write, call this method to advance the internal offset.
     *
     * <p>Usage pattern:</p>
     * <pre>
     * int offset = batch.getCurrentOffset();
     * long ptr = batch.getCurrentPointer();
     * // CustomUnsafeRowJoiner writes to (ptr + offset)
     * joiner.join(row1, row2, ptr + offset);
     * // Update the batch's internal state
     * batch.updateAppendedRowSize(resultRow.getSizeInBytes());
     * </pre>
     *
     * @param rowSize The size of the row that was written (in bytes)
     * @return The offset where the row was stored (same as getCurrentOffset() before this call)
     */
    public int updateAppendedRowSize(int rowSize) {
        int returnedOffset = size;
        lastOffset = size;
        size += rowSize;
        return returnedOffset;
    }

    /**
     * Retrieves a row from this batch as a byte array.
     *
     * This method copies the row data from off-heap memory to a new Java byte array.
     * Note: This involves memory allocation and copying, so use sparingly for
     * performance-critical code paths.
     *
     * @param offset The offset within the batch where the row starts
     * @param len    The length of the row in bytes
     * @return A new byte array containing the row data, or null if parameters are invalid
     */
    public byte[] getRow(int offset, int len) {
        if (len < 0 || offset > size) {
            // Invalid parameters
            return null;
        }
        byte[] row = new byte[len];
        // Copy from off-heap memory to Java heap (byte array)
        Platform.copyMemory(null, rowData + offset, row, Platform.BYTE_ARRAY_OFFSET, len);
        return row;
    }

    /**
     * Checks if a row of the given size can be inserted into this batch.
     *
     * @param rowSize The size of the row to be inserted (in bytes)
     * @return true if there is enough space, false otherwise
     */
    public boolean canInsert(int rowSize) {
        return (size + rowSize) < batchSize;
    }

    /**
     * Checks if the given offset corresponds to the last appended row.
     *
     * @param offset The offset to check
     * @return true if this offset is where the last row starts
     */
    public boolean isLastRow(int offset) {
        return (offset == lastOffset);
    }

    /**
     * Returns the size of the last appended row.
     *
     * @return The size in bytes of the most recently appended row
     */
    public int getLastRowSize() {
        return size - lastOffset;
    }

    /**
     * Returns the current write offset (next available position).
     *
     * This is also the total size of data written to this batch so far.
     *
     * @return The current offset in bytes
     */
    public int getCurrentOffset() {
        return size;
    }

    /**
     * Returns the pointer to the off-heap memory block.
     *
     * This pointer can be used with Platform API for direct memory access.
     * To access a specific row, use: rowData + offset
     *
     * @return The base address of the off-heap memory block
     */
    public long getCurrentPointer() {
        return rowData;
    }

    /**
     * Returns the total capacity of this batch.
     *
     * @return The batch size in bytes
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns the amount of free space remaining in this batch.
     *
     * @return The remaining capacity in bytes
     */
    public int getRemainingSpace() {
        return batchSize - size;
    }

    /**
     * Frees the off-heap memory allocated by this batch.
     *
     * This method should be called when the batch is no longer needed.
     * After calling this method, the batch should not be used.
     *
     * <p>Note: This method is idempotent - calling it multiple times is safe.</p>
     */
    public void free() {
        if (!freed && rowData != -1) {
            Platform.freeMemory(rowData);
            rowData = -1;
            freed = true;
        }
    }

    /**
     * Implements AutoCloseable interface.
     * Calls {@link #free()} to release off-heap memory.
     *
     * <p>Usage with try-with-resources:</p>
     * <pre>
     * try (RowBatch batch = new RowBatch(1024)) {
     *     // use batch
     * } // automatically freed here
     * </pre>
     */
    @Override
    public void close() {
        free();
    }

    /**
     * Checks if this batch's memory has been freed.
     *
     * @return true if the memory has been freed, false otherwise
     */
    public boolean isFreed() {
        return freed;
    }
}
