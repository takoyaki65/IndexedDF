package indexeddataframe

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for InternalIndexedPartition - The core data structure for storing
 * indexed partition data with on-heap storage and CTrie-based hash index.
 *
 * These tests verify:
 * - Initialization and index creation
 * - Row insertion (single and batch)
 * - Key lookup (get) with various key types
 * - Duplicate key handling (linked list traversal)
 * - Full partition iteration
 * - Memory efficiency (minimal allocation for small partitions)
 */
class InternalIndexedPartitionTest extends AnyFunSuite {

  // Test RDD ID and partition ID for memory block identification
  private val testRddId = 1
  private val testPartitionId = 0

  /**
   * Helper method to create an UnsafeRow from values and schema.
   */
  private def createUnsafeRow(schema: StructType, values: Any*): UnsafeRow = {
    val projection = UnsafeProjection.create(schema)
    val internalRow = new GenericInternalRow(values.map {
      case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case other     => other
    }.toArray)
    projection.apply(internalRow).copy()
  }

  /**
   * Helper to create attributes from a schema.
   */
  private def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }

  /**
   * Helper to create a simple partition with Int key column.
   */
  private def createIntKeyPartition(): InternalIndexedPartition = {
    val partition = new InternalIndexedPartition
    partition.initialize(testRddId, testPartitionId)
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )
    val output = schemaToAttributes(schema)
    partition.createIndex(output, 0) // index on column 0 (Int)
    partition
  }

  /**
   * Helper to create a simple partition with String key column.
   */
  private def createStringKeyPartition(): InternalIndexedPartition = {
    val partition = new InternalIndexedPartition
    partition.initialize(testRddId, testPartitionId)
    val schema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )
    val output = schemaToAttributes(schema)
    partition.createIndex(output, 0) // index on column 0 (String)
    partition
  }

  // ============================================
  // Initialization tests
  // ============================================

  test("initialize should create empty data structures") {
    val partition = new InternalIndexedPartition
    partition.initialize(testRddId, testPartitionId)

    assert(partition.index != null, "Index should be initialized")
    assert(partition.index.isEmpty, "Index should be empty")
  }

  test("createIndex should set up schema") {
    val partition = createIntKeyPartition()

    assert(partition.nRows == 0, "Should have no rows")
    assert(partition.size == 0, "Size should be 0")
  }

  // ============================================
  // Row insertion tests
  // ============================================

  test("appendRow should add a single row") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )
    val row = createUnsafeRow(schema, 42, "hello")

    partition.appendRow(row)
    partition.finishIndexing()

    assert(partition.nRows == 1, "Should have 1 row")
    assert(partition.index.size == 1, "Index should have 1 entry")
  }

  test("appendRow should handle multiple rows with different keys") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    for (i <- 1 to 10) {
      val row = createUnsafeRow(schema, i, s"value$i")
      partition.appendRow(row)
    }
    partition.finishIndexing()

    assert(partition.nRows == 10, "Should have 10 rows")
    assert(partition.index.size == 10, "Index should have 10 entries")
  }

  test("appendRow should handle duplicate keys (linked list)") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    // Insert 3 rows with the same key
    partition.appendRow(createUnsafeRow(schema, 42, "first"))
    partition.appendRow(createUnsafeRow(schema, 42, "second"))
    partition.appendRow(createUnsafeRow(schema, 42, "third"))
    partition.finishIndexing()

    assert(partition.nRows == 3, "Should have 3 rows")
    assert(partition.index.size == 1, "Index should have 1 entry (same key)")
  }

  test("appendRows should add multiple rows from iterator") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    val rows = (1 to 5).map(i => createUnsafeRow(schema, i, s"value$i"))
    partition.appendRows(rows.iterator)
    partition.finishIndexing()

    assert(partition.nRows == 5, "Should have 5 rows")
  }

  // ============================================
  // Key lookup tests
  // ============================================

  test("get should return matching rows for Int key") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, 1, "one"))
    partition.appendRow(createUnsafeRow(schema, 2, "two"))
    partition.appendRow(createUnsafeRow(schema, 3, "three"))
    partition.finishIndexing()

    val results = partition.get(2).toList
    assert(results.length == 1, "Should find 1 row")
    assert(results.head.getInt(0) == 2, "Key should be 2")
  }

  test("get should return empty iterator for non-existent key") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, 1, "one"))
    partition.finishIndexing()

    val results = partition.get(999).toList
    assert(results.isEmpty, "Should find no rows")
  }

  test("get should return all rows with duplicate keys") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, 42, "first"))
    partition.appendRow(createUnsafeRow(schema, 42, "second"))
    partition.appendRow(createUnsafeRow(schema, 42, "third"))
    partition.finishIndexing()

    // Note: RowIterator reuses the same UnsafeRow object, so we must copy each row
    val results = partition.get(42).map(_.copy()).toList
    assert(results.length == 3, "Should find 3 rows")

    // Rows are returned in reverse insertion order (newest first)
    val values = results.map(_.getUTF8String(1).toString)
    assert(values.contains("first"))
    assert(values.contains("second"))
    assert(values.contains("third"))
  }

  test("get should work with Long keys") {
    val partition = new InternalIndexedPartition
    partition.initialize(testRddId, testPartitionId)
    val schema = StructType(
      Seq(
        StructField("key", LongType),
        StructField("value", StringType)
      )
    )
    partition.createIndex(schemaToAttributes(schema), 0)

    partition.appendRow(createUnsafeRow(schema, 123456789L, "long-value"))
    partition.finishIndexing()

    val results = partition.get(123456789L).toList
    assert(results.length == 1)
    assert(results.head.getLong(0) == 123456789L)
  }

  test("get should work with String keys (hashed)") {
    val partition = createStringKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, "hello", 1))
    partition.appendRow(createUnsafeRow(schema, "world", 2))
    partition.finishIndexing()

    val results = partition.get("hello").toList
    assert(results.length == 1)
    assert(results.head.getUTF8String(0).toString == "hello")
    assert(results.head.getInt(1) == 1)
  }

  // ============================================
  // Full partition iteration tests
  // ============================================

  test("iterator should return all rows in partition") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    for (i <- 1 to 5) {
      partition.appendRow(createUnsafeRow(schema, i, s"value$i"))
    }
    partition.finishIndexing()

    val allRows = partition.iterator().toList
    assert(allRows.length == 5, "Should iterate over all 5 rows")

    val keys = allRows.map(_.getInt(0)).toSet
    assert(keys == Set(1, 2, 3, 4, 5), "Should contain all keys")
  }

  test("iterator should handle duplicate keys correctly") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, 1, "a"))
    partition.appendRow(createUnsafeRow(schema, 1, "b"))
    partition.appendRow(createUnsafeRow(schema, 2, "c"))
    partition.appendRow(createUnsafeRow(schema, 2, "d"))
    partition.finishIndexing()

    val allRows = partition.iterator().toList
    assert(allRows.length == 4, "Should iterate over all 4 rows")
  }

  test("iterator should return empty for empty partition") {
    val partition = createIntKeyPartition()
    partition.finishIndexing()

    val allRows = partition.iterator().toList
    assert(allRows.isEmpty, "Should be empty")
  }

  // ============================================
  // Multiget tests
  // ============================================

  test("multiget should return rows for multiple keys") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    for (i <- 1 to 10) {
      partition.appendRow(createUnsafeRow(schema, i, s"value$i"))
    }
    partition.finishIndexing()

    val keys = Array[AnyVal](2, 5, 8)
    val results = partition.multiget(keys).toList

    assert(results.length == 3, "Should find 3 rows")
    val foundKeys = results.map(_.getInt(0)).toSet
    assert(foundKeys == Set(2, 5, 8))
  }

  test("multiget should handle missing keys gracefully") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    partition.appendRow(createUnsafeRow(schema, 1, "one"))
    partition.appendRow(createUnsafeRow(schema, 2, "two"))
    partition.finishIndexing()

    val keys = Array[AnyVal](1, 999, 2) // 999 doesn't exist
    val results = partition.multiget(keys).toList

    assert(results.length == 2, "Should find 2 rows (skipping 999)")
  }

  // ============================================
  // Edge cases and stress tests
  // ============================================

  test("should handle many rows efficiently") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    val numRows = 10000
    for (i <- 1 to numRows) {
      partition.appendRow(createUnsafeRow(schema, i, s"value$i"))
    }
    partition.finishIndexing()

    assert(partition.nRows == numRows)
    assert(partition.index.size == numRows)

    // Verify random lookups work
    assert(partition.get(5000).toList.length == 1)
    assert(partition.get(9999).toList.length == 1)
  }

  test("should handle many duplicate keys") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    val numRows = 1000
    for (i <- 1 to numRows) {
      // All rows have the same key
      partition.appendRow(createUnsafeRow(schema, 42, s"value$i"))
    }
    partition.finishIndexing()

    assert(partition.nRows == numRows)
    assert(partition.index.size == 1, "Only one key in index")

    // All rows should be retrievable
    val results = partition.get(42).toList
    assert(results.length == numRows)
  }

  test("should create multiple pages for large data") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    // Create rows with large strings to fill pages
    val largeValue = "x" * 100000 // 100KB per row
    val numRows = 100 // ~10MB total, should require multiple 4MB pages

    for (i <- 1 to numRows) {
      partition.appendRow(createUnsafeRow(schema, i, largeValue))
    }
    partition.finishIndexing()

    assert(partition.getMemoryUsed > InternalIndexedPartition.DefaultPageSize, "Should have allocated multiple pages")
    assert(partition.nRows == numRows)

    // Verify data integrity
    for (i <- 1 to 10) {
      val results = partition.get(i).toList
      assert(results.length == 1)
    }
  }

  test("should handle rows with various data types") {
    val partition = new InternalIndexedPartition
    partition.initialize(testRddId, testPartitionId)
    val schema = StructType(
      Seq(
        StructField("intCol", IntegerType),
        StructField("longCol", LongType),
        StructField("doubleCol", DoubleType),
        StructField("stringCol", StringType)
      )
    )
    partition.createIndex(schemaToAttributes(schema), 0)

    partition.appendRow(createUnsafeRow(schema, 1, 100L, 3.14, "test"))
    partition.appendRow(createUnsafeRow(schema, 2, 200L, 2.71, "test2"))
    partition.finishIndexing()

    val results = partition.get(1).toList
    assert(results.length == 1)
    val row = results.head
    assert(row.getInt(0) == 1)
    assert(row.getLong(1) == 100L)
    assert(math.abs(row.getDouble(2) - 3.14) < 0.001)
    assert(row.getUTF8String(3).toString == "test")
  }

  test("getMemoryUsed should return correct memory usage") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    assert(partition.getMemoryUsed == 0, "Empty partition should use 0 memory")

    partition.appendRow(createUnsafeRow(schema, 1, "hello"))
    partition.finishIndexing()

    assert(partition.getMemoryUsed > 0, "After insert, should use some memory")
  }

  // ============================================
  // Memory efficiency tests
  // ============================================

  test("finishIndexing should allocate minimal memory for small partitions") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    // Insert just one small row
    partition.appendRow(createUnsafeRow(schema, 1, "small"))
    partition.finishIndexing()

    // Memory used should be much less than 4MB default page size
    val memUsed = partition.getMemoryUsed
    assert(memUsed < 1024, s"Memory used ($memUsed) should be less than 1KB for tiny partition")
    assert(memUsed > 0, "Should have allocated some memory")

    // Data should still be accessible
    val results = partition.get(1).toList
    assert(results.length == 1)
    assert(results.head.getUTF8String(1).toString == "small")
  }

  test("finishIndexing should allocate exact size needed") {
    val partition = createIntKeyPartition()
    val schema = StructType(
      Seq(
        StructField("key", IntegerType),
        StructField("value", StringType)
      )
    )

    // Insert a few rows
    for (i <- 1 to 10) {
      partition.appendRow(createUnsafeRow(schema, i, s"value$i"))
    }
    partition.finishIndexing()

    // Memory should be close to actual data size (with some overhead for headers)
    val memUsed = partition.getMemoryUsed
    val dataSize = partition.dataSize

    // Memory used should be data size + overhead (headers, prev pointers)
    // Each row has 4-byte size header + 8-byte prev pointer = 12 bytes overhead
    assert(memUsed < dataSize * 3, "Memory overhead should be reasonable")
    assert(memUsed < InternalIndexedPartition.DefaultPageSize, "Should not allocate full 4MB page")
  }

  test("empty partition should use no memory after finishIndexing") {
    val partition = createIntKeyPartition()
    partition.finishIndexing()

    assert(partition.getMemoryUsed == 0, "Empty partition should use 0 memory")
    assert(partition.getPageCount == 0, "Empty partition should have 0 pages")
  }
}
