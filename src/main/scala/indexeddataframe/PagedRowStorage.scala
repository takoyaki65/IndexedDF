package indexeddataframe

import org.apache.spark.SparkEnv
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.memory.MemoryMode
import org.apache.spark.unsafe.Platform
import scala.collection.mutable.ArrayBuffer

/**
 * Page-based row storage that manages memory through Spark's Storage Memory.
 *
 * == Overview ==
 *
 * This class allocates memory in pages (default 4MB) and tracks usage through
 * Spark's unified memory manager. Memory is allocated on-heap using byte arrays,
 * which is Spark's default memory mode and requires no special configuration.
 *
 * == Memory Layout ==
 *
 * {{{
 *   Page 0 (4MB byte array):
 *   +------------------+------------------+------------------+-----+
 *   |      Row 0       |      Row 1       |      Row 2       | ... |
 *   +------------------+------------------+------------------+-----+
 *
 *   Each row layout:
 *   +------------+---------------------------+------------+
 *   | Size (4B)  |     UnsafeRow Data        | Prev (8B)  |
 *   +------------+---------------------------+------------+
 * }}}
 *
 * == Pointer Format ==
 *
 * The index stores 64-bit addresses. For on-heap memory, this is:
 *   Platform.BYTE_ARRAY_OFFSET + offset_within_array
 * The page object reference is stored separately.
 *
 * == Thread Safety ==
 *
 * This class is NOT thread-safe. It should only be used within a single partition
 * during the indexing phase.
 *
 * == TODO: Future Improvements ==
 *
 * 1. '''BlockManager Integration''': Currently, `acquireStorageMemory` only "declares"
 *    memory usage to Spark's memory manager but does not register pages with BlockManager.
 *    This means:
 *    - Pages cannot be evicted to disk when memory is under pressure
 *    - Other Spark-managed blocks may be evicted instead, potentially causing OOM
 *    - To fix: Register pages with `BlockManager.putBytes()` using `StorageLevel.MEMORY_AND_DISK`
 *
 * 2. '''Memory Release''': The `free()` method is currently not called from anywhere.
 *    Need to determine the proper lifecycle:
 *    - Option A: Call `free()` when `unpersist()` is called on the cached DataFrame
 *    - Option B: If BlockManager integration is implemented, Spark handles eviction/cleanup
 *    - Option C: Rely on GC for on-heap arrays (current behavior, but memory accounting is off)
 *
 * @param rddId       The RDD ID for block identification
 * @param partitionId The partition ID for block identification
 * @param pageSize    Size of each page in bytes (default 4MB)
 */
class PagedRowStorage(
    val rddId: Int,
    val partitionId: Int,
    val pageSize: Long = PagedRowStorage.DefaultPageSize
) extends Serializable {

  // List of allocated page byte arrays (on-heap)
  @transient private var pages: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()

  // Current page index and offset within that page
  @transient private var currentPageIdx: Int = -1
  @transient private var currentOffset: Long = 0

  // Total memory used (for tracking and release)
  @transient private var totalMemoryUsed: Long = 0

  // Block counter for unique block IDs
  @transient private var blockCounter: Int = 0

  // Flag to track if storage has been initialized
  @transient private var initialized: Boolean = false

  /**
   * Ensures the storage is initialized. Called lazily on first use.
   */
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      pages = new ArrayBuffer[Array[Byte]]()
      currentPageIdx = -1
      currentOffset = 0
      totalMemoryUsed = 0
      blockCounter = 0
      initialized = true
    }
  }

  /**
   * Allocates memory for a row and returns the page index and offset within the page.
   *
   * If the current page doesn't have enough space, a new page is allocated.
   * If the row is larger than the default page size, a page sized to fit
   * the row is allocated.
   *
   * @param size Required bytes (including size header + row data + prev pointer)
   * @return A tuple of (pageIndex, offsetWithinPage)
   * @throws OutOfMemoryError if storage memory cannot be acquired from Spark
   */
  def allocateForRow(size: Int): (Int, Long) = {
    ensureInitialized()

    // Allocate new page if needed
    if (currentPageIdx < 0 || currentOffset + size > pageSize) {
      val requiredSize = math.max(size.toLong, pageSize)
      allocateNewPage(requiredSize.toInt)
    }

    val pageIdx = currentPageIdx
    val offset = currentOffset
    currentOffset += size
    (pageIdx, offset)
  }

  /**
   * Gets the base object (byte array) for a given page index.
   * Used with Platform methods for memory access.
   *
   * @param pageIndex The page index
   * @return The byte array for that page
   */
  def getPage(pageIndex: Int): Array[Byte] = {
    ensureInitialized()
    pages(pageIndex)
  }

  /**
   * Allocates a new page of the specified size.
   *
   * If running within a Spark task (SparkEnv is available), memory is acquired
   * through Spark's Storage Memory manager for proper tracking and limits.
   * If running outside Spark (e.g., unit tests), memory is allocated directly.
   *
   * @param size Size of the page to allocate
   * @throws OutOfMemoryError if storage memory cannot be acquired
   */
  private def allocateNewPage(size: Int): Unit = {
    // Check if we're running within Spark
    val sparkEnvOpt = Option(SparkEnv.get)

    sparkEnvOpt.foreach { sparkEnv =>
      // Create unique block ID for this page
      val blockId = RDDBlockId(rddId, partitionId * 1000000 + blockCounter)
      blockCounter += 1

      // Try to acquire storage memory from Spark (on-heap)
      val memoryManager = sparkEnv.memoryManager
      val acquired = memoryManager.acquireStorageMemory(
        blockId,
        size.toLong,
        MemoryMode.ON_HEAP
      )

      if (!acquired) {
        throw new OutOfMemoryError(
          s"Cannot acquire $size bytes of storage memory for IndexedDF. " +
            s"RDD=$rddId, Partition=$partitionId, CurrentUsage=$totalMemoryUsed bytes. " +
            s"Consider increasing spark.memory.fraction or reducing data size."
        )
      }
    }

    // Allocate on-heap byte array
    val page = new Array[Byte](size)

    pages += page
    currentPageIdx = pages.size - 1
    currentOffset = 0
    totalMemoryUsed += size
  }

  /**
   * Returns the list of all page byte arrays.
   * Used for snapshot/copy-on-write operations.
   */
  def getPages: Seq[Array[Byte]] = {
    ensureInitialized()
    pages.toSeq
  }

  /**
   * Returns the total memory used by this storage.
   */
  def getMemoryUsed: Long = totalMemoryUsed

  /**
   * Returns the number of pages allocated.
   */
  def getPageCount: Int = {
    ensureInitialized()
    pages.size
  }

  /**
   * Frees all allocated memory and releases storage memory back to Spark.
   *
   * This method should be called when the partition is no longer needed.
   * After calling this method, the storage should not be used.
   */
  def free(): Unit = {
    if (!initialized || pages == null) return

    // Release storage memory back to Spark (if available)
    // On-heap memory will be garbage collected automatically
    if (totalMemoryUsed > 0) {
      Option(SparkEnv.get).foreach { sparkEnv =>
        sparkEnv.memoryManager.releaseStorageMemory(totalMemoryUsed, MemoryMode.ON_HEAP)
      }
    }

    pages.clear()
    totalMemoryUsed = 0
    currentPageIdx = -1
    currentOffset = 0
    initialized = false
  }

  /**
   * Creates a snapshot of this storage for copy-on-write semantics.
   *
   * The snapshot shares existing pages with the original but can allocate
   * new pages independently. Existing pages are treated as read-only.
   *
   * @return A new PagedRowStorage that shares existing data
   */
  def snapshot(): PagedRowStorage = {
    ensureInitialized()

    val copy = new PagedRowStorage(rddId, partitionId, pageSize)
    copy.ensureInitialized()

    // Share existing pages (read-only)
    copy.pages ++= this.pages
    copy.currentPageIdx = this.currentPageIdx
    copy.currentOffset = this.currentOffset
    copy.totalMemoryUsed = this.totalMemoryUsed
    copy.blockCounter = this.blockCounter

    // Force new page allocation on next write by setting offset to page size
    // This ensures copy-on-write: new writes go to new pages
    if (copy.currentPageIdx >= 0) {
      copy.currentOffset = pageSize
    }

    copy
  }
}

object PagedRowStorage {

  /** Default page size: 4MB */
  val DefaultPageSize: Long = 4 * 1024 * 1024
}
