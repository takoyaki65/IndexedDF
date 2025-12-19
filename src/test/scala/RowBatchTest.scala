package indexeddataframe

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for RowBatch - Off-heap memory storage for UnsafeRow data.
 *
 * These tests verify:
 * - Basic append and retrieve operations
 * - Capacity management (canInsert, remaining space)
 * - Multiple rows in a single batch
 * - Edge cases (empty rows, full batch)
 * - Integration with UnsafeRow
 */
class RowBatchTest extends AnyFunSuite {

  // Use a small batch size for testing (1KB instead of 4MB)
  val testBatchSize: Int = 1024

  test("constructor should allocate off-heap memory") {
    val batch = new RowBatch(testBatchSize)

    assert(batch.rowData != -1, "rowData should be a valid memory address")
    assert(batch.getCurrentOffset == 0, "Initial offset should be 0")
    assert(batch.getCurrentPointer == batch.rowData, "Pointer should equal rowData")
    assert(batch.getBatchSize == testBatchSize, "Batch size should match constructor argument")
    assert(batch.getRemainingSpace == testBatchSize, "Initial remaining space should equal batch size")
  }

  test("appendRow should store data and return correct offset") {
    val batch = new RowBatch(testBatchSize)
    val row1 = Array[Byte](1, 2, 3, 4, 5)
    val row2 = Array[Byte](10, 20, 30)

    // Append first row
    val offset1 = batch.appendRow(row1)
    assert(offset1 == 0, "First row should start at offset 0")
    assert(batch.getCurrentOffset == 5, "Offset should advance by row size")
    assert(batch.getLastRowSize == 5, "Last row size should be 5")
    assert(batch.isLastRow(0), "Offset 0 should be the last row")

    // Append second row
    val offset2 = batch.appendRow(row2)
    assert(offset2 == 5, "Second row should start at offset 5")
    assert(batch.getCurrentOffset == 8, "Offset should advance to 8")
    assert(batch.getLastRowSize == 3, "Last row size should be 3")
    assert(batch.isLastRow(5), "Offset 5 should be the last row")
    assert(!batch.isLastRow(0), "Offset 0 should no longer be the last row")
  }

  test("getRow should retrieve stored data correctly") {
    val batch = new RowBatch(testBatchSize)
    val originalRow = Array[Byte](100, -50, 0, 127, -128)

    val offset = batch.appendRow(originalRow)
    val retrievedRow = batch.getRow(offset, originalRow.length)

    assert(retrievedRow != null, "Retrieved row should not be null")
    assert(retrievedRow.length == originalRow.length, "Retrieved row should have same length")
    assert(retrievedRow.sameElements(originalRow), "Retrieved data should match original")
  }

  test("getRow should return null for invalid parameters") {
    val batch = new RowBatch(testBatchSize)
    val row = Array[Byte](1, 2, 3)
    batch.appendRow(row)

    // Negative length
    assert(batch.getRow(0, -1) == null, "Should return null for negative length")

    // Offset beyond written data
    assert(batch.getRow(100, 3) == null, "Should return null for offset beyond data")
  }

  test("canInsert should correctly check available space") {
    val smallBatchSize = 100
    val batch = new RowBatch(smallBatchSize)

    // Initially should have space
    assert(batch.canInsert(50), "Should have space for 50 bytes")
    assert(batch.canInsert(99), "Should have space for 99 bytes")
    assert(!batch.canInsert(100), "Should NOT have space for 100 bytes (>= check)")
    assert(!batch.canInsert(101), "Should NOT have space for 101 bytes")

    // After appending, space should decrease
    batch.appendRow(new Array[Byte](50))
    assert(batch.canInsert(49), "Should have space for 49 bytes after appending 50")
    assert(!batch.canInsert(50), "Should NOT have space for 50 bytes after appending 50")
  }

  test("updateAppendedRowSize should advance offset without copying data") {
    val batch = new RowBatch(testBatchSize)

    // Simulate the pattern used with CustomUnsafeRowJoiner
    val offset1 = batch.getCurrentOffset
    assert(offset1 == 0)

    // Pretend we wrote 24 bytes directly to off-heap memory
    val returnedOffset = batch.updateAppendedRowSize(24)
    assert(returnedOffset == 0, "Should return the offset before update")
    assert(batch.getCurrentOffset == 24, "Offset should advance by 24")
    assert(batch.getLastRowSize == 24, "Last row size should be 24")

    // Append another row
    val offset2 = batch.updateAppendedRowSize(16)
    assert(offset2 == 24, "Second row should start at offset 24")
    assert(batch.getCurrentOffset == 40, "Total offset should be 40")
  }

  test("getRemainingSpace should return correct value") {
    val batch = new RowBatch(testBatchSize)

    assert(batch.getRemainingSpace == testBatchSize)

    batch.appendRow(new Array[Byte](100))
    assert(batch.getRemainingSpace == testBatchSize - 100)

    batch.appendRow(new Array[Byte](200))
    assert(batch.getRemainingSpace == testBatchSize - 300)
  }

  test("multiple rows should be stored and retrieved independently") {
    val batch = new RowBatch(testBatchSize)

    val rows = Seq(
      Array[Byte](1, 2, 3),
      Array[Byte](4, 5, 6, 7, 8),
      Array[Byte](9),
      Array[Byte](10, 11, 12, 13)
    )

    // Store all rows and remember offsets
    val offsets = rows.map(row => batch.appendRow(row))

    // Verify each row can be retrieved correctly
    rows.zip(offsets).foreach { case (originalRow, offset) =>
      val retrieved = batch.getRow(offset, originalRow.length)
      assert(retrieved.sameElements(originalRow),
        s"Row at offset $offset should match original")
    }
  }

  test("should work with UnsafeRow byte representation") {
    val batch = new RowBatch(testBatchSize)

    // Create a simple UnsafeRow with 2 columns
    val unsafeRow = new UnsafeRow(2)
    val rowBytes = new Array[Byte](24) // 8 bytes null bitmap + 8 bytes per column
    unsafeRow.pointTo(rowBytes, 24)
    unsafeRow.setInt(0, 42)
    unsafeRow.setLong(1, 123456789L)

    // Store the UnsafeRow bytes
    val offset = batch.appendRow(rowBytes)

    // Retrieve and verify
    val retrieved = batch.getRow(offset, rowBytes.length)
    val retrievedRow = new UnsafeRow(2)
    retrievedRow.pointTo(retrieved, retrieved.length)

    assert(retrievedRow.getInt(0) == 42, "First column should be 42")
    assert(retrievedRow.getLong(1) == 123456789L, "Second column should be 123456789")
  }

  test("direct memory write pattern (as used with CustomUnsafeRowJoiner)") {
    val batch = new RowBatch(testBatchSize)

    // Get the pointer and offset
    val ptr = batch.getCurrentPointer
    val offset = batch.getCurrentOffset

    // Write directly to off-heap memory using Platform API
    val testData = Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte)
    Platform.copyMemory(
      testData, Platform.BYTE_ARRAY_OFFSET,
      null, ptr + offset,
      testData.length
    )

    // Update the batch state
    batch.updateAppendedRowSize(testData.length)

    // Verify we can read it back
    val retrieved = batch.getRow(offset, testData.length)
    assert(retrieved.sameElements(testData), "Direct-written data should be retrievable")
  }

  test("empty row should be handled correctly") {
    val batch = new RowBatch(testBatchSize)

    val emptyRow = new Array[Byte](0)
    val offset = batch.appendRow(emptyRow)

    assert(offset == 0, "Empty row should be at offset 0")
    assert(batch.getCurrentOffset == 0, "Offset should not advance for empty row")
    assert(batch.getLastRowSize == 0, "Last row size should be 0")
  }

  test("batch should handle rows up to capacity") {
    val smallBatchSize = 100
    val batch = new RowBatch(smallBatchSize)

    // Fill up the batch with rows
    var totalSize = 0
    var rowCount = 0
    while (batch.canInsert(10)) {
      batch.appendRow(new Array[Byte](10))
      totalSize += 10
      rowCount += 1
    }

    assert(totalSize == 90, "Should have written 90 bytes (9 rows of 10 bytes)")
    assert(rowCount == 9, "Should have written 9 rows")
    assert(batch.getCurrentOffset == 90, "Offset should be 90")
    assert(batch.getRemainingSpace == 10, "Should have 10 bytes remaining")
    assert(!batch.canInsert(10), "Should not be able to insert 10 more bytes")
    assert(batch.canInsert(9), "Should be able to insert 9 bytes")
  }

  // ============================================
  // Memory management tests
  // ============================================

  test("free should release off-heap memory") {
    val batch = new RowBatch(testBatchSize)
    val originalPointer = batch.rowData

    assert(!batch.isFreed, "Batch should not be freed initially")
    assert(originalPointer != -1, "Pointer should be valid")

    batch.free()

    assert(batch.isFreed, "Batch should be marked as freed")
    assert(batch.rowData == -1, "Pointer should be invalidated")
  }

  test("free should be idempotent (safe to call multiple times)") {
    val batch = new RowBatch(testBatchSize)

    batch.free()
    assert(batch.isFreed)

    // Calling free again should not cause any issues
    batch.free()
    batch.free()
    assert(batch.isFreed)
  }

  test("close should free memory (AutoCloseable)") {
    val batch = new RowBatch(testBatchSize)

    assert(!batch.isFreed)

    batch.close()

    assert(batch.isFreed)
    assert(batch.rowData == -1)
  }

  test("try-with-resources pattern should work") {
    var freedCheck = false

    // Simulate try-with-resources in Scala
    val batch = new RowBatch(testBatchSize)
    try {
      batch.appendRow(Array[Byte](1, 2, 3))
      assert(!batch.isFreed)
    } finally {
      batch.close()
      freedCheck = batch.isFreed
    }

    assert(freedCheck, "Batch should be freed after close")
  }
}
