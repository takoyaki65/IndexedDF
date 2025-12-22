package indexeddataframe

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for CustomUnsafeRowJoiner - A code generator that joins two UnsafeRows
 * into a single UnsafeRow using an externally provided on-heap memory buffer.
 *
 * These tests verify:
 * - Basic row joining with fixed-length types (Int, Long, Double)
 * - Joining rows with variable-length types (String, Array)
 * - NULL value handling
 * - Bitset alignment edge cases (column counts that are/aren't multiples of 64)
 * - Memory buffer usage patterns
 * - Schema combinations (empty schemas, many columns)
 */
class CustomUnsafeRowJoinerTest extends AnyFunSuite {

  // Buffer size for test cases (4KB should be enough for test rows)
  val testBufferSize: Int = 4096

  /**
   * Helper method to allocate an on-heap buffer for testing.
   * Returns a tuple of (byte array, base offset).
   */
  private def allocateBuffer(size: Int): (Array[Byte], Long) = {
    (new Array[Byte](size), Platform.BYTE_ARRAY_OFFSET)
  }

  /**
   * Helper method to create an UnsafeRow from values and schema.
   */
  private def createUnsafeRow(schema: StructType, values: Any*): UnsafeRow = {
    val projection = UnsafeProjection.create(schema)
    val internalRow = new GenericInternalRow(values.map {
      case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case arr: Array[Int] =>
        org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(arr)
      case other => other
    }.toArray)
    projection.apply(internalRow).copy()
  }

  // ============================================
  // Basic fixed-length type tests
  // ============================================

  test("join two rows with single Int column each") {
    val schema1 = StructType(Seq(StructField("a", IntegerType)))
    val schema2 = StructType(Seq(StructField("b", IntegerType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 42)
    val row2 = createUnsafeRow(schema2, 100)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2, "Result should have 2 fields")
    assert(result.getInt(0) == 42, "First field should be 42")
    assert(result.getInt(1) == 100, "Second field should be 100")
  }

  test("join two rows with multiple Int columns") {
    val schema1 = StructType(
      Seq(
        StructField("a1", IntegerType),
        StructField("a2", IntegerType),
        StructField("a3", IntegerType)
      )
    )
    val schema2 = StructType(
      Seq(
        StructField("b1", IntegerType),
        StructField("b2", IntegerType)
      )
    )

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 1, 2, 3)
    val row2 = createUnsafeRow(schema2, 4, 5)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 5)
    assert(result.getInt(0) == 1)
    assert(result.getInt(1) == 2)
    assert(result.getInt(2) == 3)
    assert(result.getInt(3) == 4)
    assert(result.getInt(4) == 5)
  }

  test("join rows with Long columns") {
    val schema1 = StructType(Seq(StructField("a", LongType)))
    val schema2 = StructType(Seq(StructField("b", LongType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 9876543210L)
    val row2 = createUnsafeRow(schema2, 1234567890L)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(result.getLong(0) == 9876543210L)
    assert(result.getLong(1) == 1234567890L)
  }

  test("join rows with Double columns") {
    val schema1 = StructType(Seq(StructField("a", DoubleType)))
    val schema2 = StructType(Seq(StructField("b", DoubleType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 3.14159)
    val row2 = createUnsafeRow(schema2, 2.71828)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(math.abs(result.getDouble(0) - 3.14159) < 0.00001)
    assert(math.abs(result.getDouble(1) - 2.71828) < 0.00001)
  }

  test("join rows with mixed fixed-length types") {
    val schema1 = StructType(
      Seq(
        StructField("intCol", IntegerType),
        StructField("longCol", LongType)
      )
    )
    val schema2 = StructType(
      Seq(
        StructField("doubleCol", DoubleType),
        StructField("boolCol", BooleanType)
      )
    )

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 42, 123456789L)
    val row2 = createUnsafeRow(schema2, 9.99, true)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 4)
    assert(result.getInt(0) == 42)
    assert(result.getLong(1) == 123456789L)
    assert(math.abs(result.getDouble(2) - 9.99) < 0.001)
    assert(result.getBoolean(3) == true)
  }

  // ============================================
  // Variable-length type tests (String)
  // ============================================

  test("join two rows with String columns") {
    val schema1 = StructType(Seq(StructField("a", StringType)))
    val schema2 = StructType(Seq(StructField("b", StringType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, "hello")
    val row2 = createUnsafeRow(schema2, "world")

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(result.getUTF8String(0).toString == "hello")
    assert(result.getUTF8String(1).toString == "world")
  }

  test("join rows with String and Int columns") {
    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )
    val schema2 = StructType(
      Seq(
        StructField("value", IntegerType),
        StructField("description", StringType)
      )
    )

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 1, "Alice")
    val row2 = createUnsafeRow(schema2, 100, "Customer")

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 4)
    assert(result.getInt(0) == 1)
    assert(result.getUTF8String(1).toString == "Alice")
    assert(result.getInt(2) == 100)
    assert(result.getUTF8String(3).toString == "Customer")
  }

  test("join rows with empty String") {
    val schema1 = StructType(Seq(StructField("a", StringType)))
    val schema2 = StructType(Seq(StructField("b", StringType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, "")
    val row2 = createUnsafeRow(schema2, "nonempty")

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(result.getUTF8String(0).toString == "")
    assert(result.getUTF8String(1).toString == "nonempty")
  }

  test("join rows with long Strings") {
    val schema1 = StructType(Seq(StructField("a", StringType)))
    val schema2 = StructType(Seq(StructField("b", StringType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val longString1 = "a" * 500
    val longString2 = "b" * 300
    val row1 = createUnsafeRow(schema1, longString1)
    val row2 = createUnsafeRow(schema2, longString2)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(result.getUTF8String(0).toString == longString1)
    assert(result.getUTF8String(1).toString == longString2)
  }

  // ============================================
  // NULL value handling tests
  // ============================================

  test("join rows with NULL Int values") {
    val schema1 = StructType(Seq(StructField("a", IntegerType, nullable = true)))
    val schema2 = StructType(Seq(StructField("b", IntegerType, nullable = true)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, null)
    val row2 = createUnsafeRow(schema2, 42)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(result.isNullAt(0), "First field should be NULL")
    assert(!result.isNullAt(1), "Second field should not be NULL")
    assert(result.getInt(1) == 42)
  }

  test("join rows with NULL String values") {
    val schema1 = StructType(Seq(StructField("a", StringType, nullable = true)))
    val schema2 = StructType(Seq(StructField("b", StringType, nullable = true)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, "hello")
    val row2 = createUnsafeRow(schema2, null)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 2)
    assert(!result.isNullAt(0))
    assert(result.getUTF8String(0).toString == "hello")
    assert(result.isNullAt(1), "Second field should be NULL")
  }

  test("join rows with multiple NULL values in different positions") {
    val schema1 = StructType(
      Seq(
        StructField("a1", IntegerType, nullable = true),
        StructField("a2", StringType, nullable = true),
        StructField("a3", LongType, nullable = true)
      )
    )
    val schema2 = StructType(
      Seq(
        StructField("b1", StringType, nullable = true),
        StructField("b2", IntegerType, nullable = true)
      )
    )

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, null, "test", null)
    val row2 = createUnsafeRow(schema2, null, 99)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 5)
    assert(result.isNullAt(0))
    assert(result.getUTF8String(1).toString == "test")
    assert(result.isNullAt(2))
    assert(result.isNullAt(3))
    assert(result.getInt(4) == 99)
  }

  // ============================================
  // Bitset alignment tests
  // ============================================

  test("join with row1 having column count as multiple of 64") {
    // 64 columns in row1 - bitset is exactly 1 word, no bit shifting needed
    val fields1 = (0 until 64).map(i => StructField(s"a$i", IntegerType))
    val schema1 = StructType(fields1)
    val schema2 = StructType(Seq(StructField("b", IntegerType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val values1 = (0 until 64).map(_.asInstanceOf[Any])
    val row1 = createUnsafeRow(schema1, values1: _*)
    val row2 = createUnsafeRow(schema2, 999)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 65)
    // Verify first and last values from row1
    assert(result.getInt(0) == 0)
    assert(result.getInt(63) == 63)
    // Verify row2's value
    assert(result.getInt(64) == 999)
  }

  test("join with row1 having column count not multiple of 64") {
    // 3 columns in row1 - bitset has 61 unused bits that need to be filled with row2's bits
    val schema1 = StructType(
      Seq(
        StructField("a1", IntegerType),
        StructField("a2", IntegerType),
        StructField("a3", IntegerType)
      )
    )
    val schema2 = StructType(
      Seq(
        StructField("b1", IntegerType),
        StructField("b2", IntegerType)
      )
    )

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 1, 2, 3)
    val row2 = createUnsafeRow(schema2, 4, 5)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 5)
    assert(result.getInt(0) == 1)
    assert(result.getInt(1) == 2)
    assert(result.getInt(2) == 3)
    assert(result.getInt(3) == 4)
    assert(result.getInt(4) == 5)
  }

  test("join with both rows spanning multiple bitset words") {
    // Row1: 65 columns (2 words), Row2: 65 columns (2 words)
    // Output: 130 columns (3 words) - tests complex bit shifting
    val fields1 = (0 until 65).map(i => StructField(s"a$i", IntegerType))
    val fields2 = (0 until 65).map(i => StructField(s"b$i", IntegerType))
    val schema1 = StructType(fields1)
    val schema2 = StructType(fields2)

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val values1 = (0 until 65).map(_.asInstanceOf[Any])
    val values2 = (100 until 165).map(_.asInstanceOf[Any])
    val row1 = createUnsafeRow(schema1, values1: _*)
    val row2 = createUnsafeRow(schema2, values2: _*)

    val (buf, offset) = allocateBuffer(testBufferSize * 4) // Need more space for many columns
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 130)
    // Verify boundary values
    assert(result.getInt(0) == 0)
    assert(result.getInt(64) == 64)
    assert(result.getInt(65) == 100)
    assert(result.getInt(129) == 164)
  }

  // ============================================
  // Edge cases
  // ============================================

  test("join with empty row2 schema") {
    val schema1 = StructType(Seq(StructField("a", IntegerType)))
    val schema2 = StructType(Seq.empty)

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 42)
    val row2 = createUnsafeRow(schema2)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 1)
    assert(result.getInt(0) == 42)
  }

  test("join with empty row1 schema") {
    val schema1 = StructType(Seq.empty)
    val schema2 = StructType(Seq(StructField("b", IntegerType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1)
    val row2 = createUnsafeRow(schema2, 99)

    val (buf, offset) = allocateBuffer(testBufferSize)
    val result = joiner.join(row1, row2, buf, offset)

    assert(result.numFields() == 1)
    assert(result.getInt(0) == 99)
  }

  test("join same rows multiple times with same buffer") {
    val schema1 = StructType(Seq(StructField("a", IntegerType)))
    val schema2 = StructType(Seq(StructField("b", IntegerType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)
    val row1 = createUnsafeRow(schema1, 10)
    val row2 = createUnsafeRow(schema2, 20)

    val (buf, offset) = allocateBuffer(testBufferSize)
    // Join multiple times - the joiner should reuse its internal UnsafeRow
    for (i <- 1 to 100) {
      val result = joiner.join(row1, row2, buf, offset)
      assert(result.getInt(0) == 10, s"Failed on iteration $i")
      assert(result.getInt(1) == 20, s"Failed on iteration $i")
    }
  }

  test("join different row pairs with same joiner") {
    val schema1 = StructType(Seq(StructField("a", IntegerType)))
    val schema2 = StructType(Seq(StructField("b", IntegerType)))

    val joiner = GenerateCustomUnsafeRowJoiner.create(schema1, schema2)

    val (buf, offset) = allocateBuffer(testBufferSize)
    for (i <- 1 to 10) {
      val row1 = createUnsafeRow(schema1, i)
      val row2 = createUnsafeRow(schema2, i * 100)
      val result = joiner.join(row1, row2, buf, offset)
      assert(result.getInt(0) == i)
      assert(result.getInt(1) == i * 100)
    }
  }
}
