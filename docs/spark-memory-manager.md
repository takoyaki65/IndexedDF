# Apache Spark Memory Manager Overview

## Table of Contents

1. [Introduction](#introduction)
2. [Memory Architecture Overview](#memory-architecture-overview)
3. [Key Components](#key-components)
4. [Implementing MemoryConsumer](#implementing-memoryconsumer)
5. [Integration Strategy for IndexedDF](#integration-strategy-for-indexeddf)
6. [References](#references)

---

## Introduction

Apache Spark has a `MemoryManager` that provides unified management of memory for task execution and storage. Currently, IndexedDF allocates off-heap memory directly via `Platform.allocateMemory()`, which operates independently of Spark's memory management. This causes several issues:

- Spark cannot track memory usage
- Spark cannot request spill (eviction to disk) when memory is low
- Increased risk of OOM errors

This document explains the Spark Memory Manager architecture and how to integrate IndexedDF with it.

---

## Memory Architecture Overview

### Unified Memory Manager

Since Spark 1.6, `UnifiedMemoryManager` is the default memory manager. It divides memory into two regions:

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark Memory Pool                         │
├─────────────────────────────────────┬───────────────────────┤
│         Execution Memory            │    Storage Memory     │
│   (shuffles, joins, sorts, aggs)    │  (caching, broadcast) │
├─────────────────────────────────────┴───────────────────────┤
│              Boundary moves dynamically (Unified)            │
└─────────────────────────────────────────────────────────────┘
```

- **Execution Memory**: Used for shuffles, joins, sorts, and aggregations during task execution
- **Storage Memory**: Used for caching data and broadcast variables

The boundary between these regions is dynamic; when one region is low on memory, it can borrow from the other.

### Memory Mode (ON_HEAP / OFF_HEAP)

```scala
// Selection logic inside Spark's MemoryManager
val tungstenMemoryMode: MemoryMode = {
  if (conf.get(MEMORY_OFFHEAP_ENABLED) && conf.get(MEMORY_OFFHEAP_SIZE) > 0) {
    MemoryMode.OFF_HEAP
  } else {
    MemoryMode.ON_HEAP
  }
}
```

| Configuration | Description |
|---------------|-------------|
| `spark.memory.offHeap.enabled` | Set to `true` to enable OFF_HEAP mode |
| `spark.memory.offHeap.size` | Maximum OFF_HEAP memory size (e.g., `2g`) |

---

## Key Components

### 1. MemoryManager

The abstraction layer for memory management. It manages four memory pools:

```
MemoryManager
├── onHeapStorageMemoryPool
├── offHeapStorageMemoryPool
├── onHeapExecutionMemoryPool
└── offHeapExecutionMemoryPool
```

**Key Methods:**

| Method | Description |
|--------|-------------|
| `acquireExecutionMemory(numBytes, taskAttemptId, memoryMode)` | Acquire memory for task execution |
| `acquireStorageMemory(blockId, numBytes, memoryMode)` | Acquire memory for storage |
| `releaseExecutionMemory(numBytes, taskAttemptId, memoryMode)` | Release execution memory |
| `releaseStorageMemory(numBytes, memoryMode)` | Release storage memory |

**How to Access:**

```scala
import org.apache.spark.SparkEnv
val memoryManager = SparkEnv.get.memoryManager
```

### 2. TaskMemoryManager

Manages memory for individual tasks. It mediates memory requests from `MemoryConsumer` instances.

**Key Features:**
- Page table management (up to 8192 pages)
- 64-bit address encoding (upper 13 bits: page number, lower 51 bits: offset)
- Triggers spill when memory is insufficient

**How to Obtain:**

```scala
import org.apache.spark.TaskContext

val taskContext = TaskContext.get()
val taskMemoryManager = taskContext.taskMemoryManager()
```

### 3. MemoryConsumer

An abstract class for components that consume memory. Extend this class to implement custom memory management.

```java
public abstract class MemoryConsumer {
    protected final TaskMemoryManager taskMemoryManager;
    protected final long pageSize;
    protected final MemoryMode mode;
    private final AtomicLong used;

    // Acquire memory
    public long acquireMemory(long size);

    // Release memory
    public void freeMemory(long size);

    // Spill to disk when memory is low (must be implemented by subclass)
    public abstract long spill(long size, MemoryConsumer trigger) throws IOException;
}
```

---

## Implementing MemoryConsumer

### Basic Implementation Pattern

```java
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import java.io.IOException;

public class MyMemoryConsumer extends MemoryConsumer {

    private long allocatedMemory = 0;

    public MyMemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
        super(taskMemoryManager, mode);
    }

    /**
     * Allocate memory
     */
    public void allocate(long size) {
        // Request memory from Spark's MemoryManager
        long granted = acquireMemory(size);

        if (granted < size) {
            // Handle case where requested memory was not fully granted
            // Only allocate the granted amount of actual off-heap memory
        }

        // Perform actual memory allocation
        allocatedMemory = granted;
    }

    /**
     * Release memory
     */
    public void release() {
        if (allocatedMemory > 0) {
            freeMemory(allocatedMemory);
            allocatedMemory = 0;
        }
    }

    /**
     * Spill data to disk when memory is low.
     * Called by TaskMemoryManager.
     *
     * @param size Number of bytes requested to be freed
     * @param trigger The MemoryConsumer that triggered the spill
     * @return Number of bytes actually freed
     */
    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger == this) {
            // Don't spill if we are the trigger
            return 0;
        }

        // Write data to disk
        long freedBytes = spillToDisk(size);

        // Release memory
        freeMemory(freedBytes);
        allocatedMemory -= freedBytes;

        return freedBytes;
    }

    private long spillToDisk(long size) {
        // Actual spill implementation
        // Write data to disk and free off-heap memory
        return size;
    }
}
```

### Usage Example (Within a Task)

```scala
import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryMode

// Execute within a task
val taskContext = TaskContext.get()
val taskMemoryManager = taskContext.taskMemoryManager()

val consumer = new MyMemoryConsumer(taskMemoryManager, MemoryMode.OFF_HEAP)

try {
  // Allocate memory
  consumer.allocate(4 * 1024 * 1024) // 4MB

  // Perform operations using the memory
  // ...

} finally {
  // Release memory when task ends
  consumer.release()
}
```

### Automatic Cleanup on Task Completion

```scala
val taskContext = TaskContext.get()

// Register a listener to release resources when task completes
taskContext.addTaskCompletionListener { context =>
  consumer.release()
}
```

---

## Integration Strategy for IndexedDF

### Current Problem

`RowBatch.java` allocates off-heap memory directly:

```java
// Current implementation
public RowBatch(int batchSize) {
    this.batchSize = batchSize;
    rowData = Platform.allocateMemory(batchSize);  // Not reported to Spark
    Platform.setMemory(rowData, (byte)0, batchSize);
}
```

### Option 1: Extend MemoryConsumer

Implement `RowBatch` as a subclass of `MemoryConsumer`:

```java
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

public class ManagedRowBatch extends MemoryConsumer implements AutoCloseable {

    private static final int DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;
    private long rowData = -1;
    private int size = 0;
    private int batchSize;

    public ManagedRowBatch(TaskMemoryManager taskMemoryManager, int batchSize) {
        super(taskMemoryManager, MemoryMode.OFF_HEAP);
        this.batchSize = batchSize;

        // Request memory from Spark
        long granted = acquireMemory(batchSize);
        if (granted < batchSize) {
            // Handle insufficient memory
            this.batchSize = (int) granted;
        }

        // Allocate off-heap memory
        rowData = Platform.allocateMemory(this.batchSize);
        Platform.setMemory(rowData, (byte)0, this.batchSize);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        // Return 0 if RowBatch doesn't support spilling
        // Or implement disk eviction logic
        return 0;
    }

    @Override
    public void close() {
        if (rowData != -1) {
            Platform.freeMemory(rowData);
            freeMemory(batchSize);  // Notify Spark of release
            rowData = -1;
        }
    }
}
```

### Option 2: Modify Existing RowBatch

Add `TaskMemoryManager` notification to the existing `RowBatch`:

```java
public class RowBatch implements AutoCloseable {

    private TaskMemoryManager taskMemoryManager;

    public RowBatch(int batchSize, TaskMemoryManager taskMemoryManager) {
        this.batchSize = batchSize;
        this.taskMemoryManager = taskMemoryManager;

        if (taskMemoryManager != null) {
            // Notify Spark of memory usage
            taskMemoryManager.acquireExecutionMemory(batchSize, this);
        }

        rowData = Platform.allocateMemory(batchSize);
        Platform.setMemory(rowData, (byte)0, batchSize);
    }

    @Override
    public void close() {
        if (!freed && rowData != -1) {
            Platform.freeMemory(rowData);

            if (taskMemoryManager != null) {
                // Notify Spark of memory release
                taskMemoryManager.releaseExecutionMemory(batchSize, this);
            }

            rowData = -1;
            freed = true;
        }
    }
}
```

### Considerations

1. **TaskMemoryManager Lifecycle**
   - `TaskMemoryManager` exists per task
   - `InternalIndexedPartition` may be shared across multiple tasks
   - Cached data persists after the original task completes

2. **Spill Implementation**
   - Current `RowBatch` does not support spilling
   - Logic to evict data to disk when memory is low is needed
   - Can reference the `Spillable` trait for implementation

3. **Managing as Storage Memory**
   - Cached data should be managed as `Storage Memory`
   - Consider integration with `MemoryStore` and `BlockManager`

---

## References

- [MemoryManager - The Internals of Spark Core](https://books.japila.pl/apache-spark-internals/memory/MemoryManager/)
- [TaskMemoryManager - The Internals of Spark Core](https://books.japila.pl/apache-spark-internals/memory/TaskMemoryManager/)
- [Spark MemoryManager.scala (GitHub)](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/memory/MemoryManager.scala)
- [Spark TaskMemoryManager.java (GitHub)](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/memory/TaskMemoryManager.java)
- [Spark MemoryConsumer.java (GitHub)](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/memory/MemoryConsumer.java)
- [Apache Spark and off-heap memory](https://www.waitingforcode.com/apache-spark/apache-spark-off-heap-memory/read)
- [Deep Dive into Spark Memory Management](https://luminousmen.com/post/dive-into-spark-memory/)
