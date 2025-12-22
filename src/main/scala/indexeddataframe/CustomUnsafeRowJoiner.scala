/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * THIS IS A CUSTOM IMPLEMENTATION OF AN UNSAFE ROW JOINER
 * WHICH USES A BUFFER PASSED AS A PARAMETER TO JOIN THE TWO
 * ROWS INSTEAD OF ALLOCATING ITS OWN MEMORY
 */

package indexeddataframe

import org.apache.spark.sql.catalyst.expressions.codegen._

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.types.StructType

/**
 * Abstract class for joining two UnsafeRows into a single UnsafeRow.
 *
 * Key difference from Spark's standard UnsafeRowJoiner:
 * - Standard UnsafeRowJoiner allocates its own internal memory buffer
 * - This custom version uses an externally provided buffer (on-heap byte array or off-heap memory)
 *
 * This design enables direct writing of join results to PagedRowStorage's memory,
 * avoiding additional memory allocations and improving performance during IndexedJoin operations.
 */
abstract class CustomUnsafeRowJoiner {

  /**
   * Joins two UnsafeRows into one, writing to an on-heap byte array.
   *
   * @param row1      The left row (typically from the indexed table)
   * @param row2      The right row (from the table being joined)
   * @param baseObj   The byte array to write the result to
   * @param baseOffset The offset within the byte array (including Platform.BYTE_ARRAY_OFFSET)
   * @return A new UnsafeRow pointing to the buffer containing the concatenated data
   */
  def join(row1: UnsafeRow, row2: UnsafeRow, baseObj: AnyRef, baseOffset: Long): UnsafeRow
}

/**
 * Code generator that creates a specialized [[CustomUnsafeRowJoiner]] for concatenating
 * two [[UnsafeRow]]s with specific schemas into a single [[UnsafeRow]].
 *
 * == UnsafeRow Memory Layout ==
 *
 * An UnsafeRow consists of three regions stored contiguously in memory:
 *
 * {{{
 * +------------------+----------------------+---------------------+
 * |  Null Bit Set    |  Fixed-Length Data   | Variable-Length Data|
 * | (8 bytes/64 cols)|  (8 bytes per field) |   (actual bytes)    |
 * +------------------+----------------------+---------------------+
 * }}}
 *
 * - '''Null Bit Set''': One bit per column, packed into 64-bit words.
 *   Number of words = ceil(numColumns / 64)
 *
 * - '''Fixed-Length Data''': 8 bytes per column.
 *   - For fixed-length types (int, long, double): stores the actual value
 *   - For variable-length types (string, array): stores offset (upper 32 bits) and size (lower 32 bits)
 *
 * - '''Variable-Length Data''': Actual bytes for variable-length columns (strings, arrays, etc.)
 *
 * == Join Algorithm ==
 *
 * The high-level algorithm for joining two UnsafeRows:
 *
 *   1. '''Concatenate Bitsets''': Merge the two null bit sets into one.
 *      Special handling is needed when row1's column count is not a multiple of 64,
 *      requiring bit shifting to properly align row2's bits.
 *
 *   2. '''Copy Fixed-Length Data''': Copy both rows' fixed-length sections sequentially.
 *
 *   3. '''Copy Variable-Length Data''': Append row1's variable data, then row2's.
 *
 *   4. '''Update Offsets''': For variable-length columns, the offset values in the
 *      fixed-length section must be adjusted because:
 *      - The bitset size may have changed
 *      - Row2's offsets need to account for row1's variable data
 *
 * == Generated Code Structure ==
 *
 * This generator produces a specialized class like:
 * {{{
 * class SpecificUnsafeRowJoiner extends CustomUnsafeRowJoiner {
 *   def join(row1: UnsafeRow, row2: UnsafeRow, buf: Long): UnsafeRow = {
 *     // Copy bitsets with proper alignment
 *     // Copy fixed-length sections
 *     // Copy variable-length sections
 *     // Adjust offsets for variable-length fields
 *     // Return UnsafeRow pointing to buf
 *   }
 * }
 * }}}
 *
 * The generated code is compiled at runtime using Spark's CodeGenerator framework
 * and cached for reuse with the same schema pair.
 */
object GenerateCustomUnsafeRowJoiner extends CodeGenerator[(StructType, StructType), CustomUnsafeRowJoiner] {

  /**
   * Creates a CustomUnsafeRowJoiner for the given schema pair.
   * This method is called by the CodeGenerator caching mechanism.
   */
  override protected def create(in: (StructType, StructType)): CustomUnsafeRowJoiner = {
    create(in._1, in._2)
  }

  /**
   * Returns the input as-is since no canonicalization is needed for schema pairs.
   */
  override protected def canonicalize(in: (StructType, StructType)): (StructType, StructType) = in

  /**
   * Returns the input as-is since schema pairs don't require binding to input schemas.
   */
  override protected def bind(in: (StructType, StructType), inputSchema: Seq[Attribute]): (StructType, StructType) = {
    in
  }

  /**
   * Creates a specialized CustomUnsafeRowJoiner for the given schemas.
   *
   * @param schema1 Schema of the left row (from indexed table)
   * @param schema2 Schema of the right row (from joined table)
   * @return A compiled joiner optimized for these specific schemas
   */
  def create(schema1: StructType, schema2: StructType): CustomUnsafeRowJoiner = {
    val ctx = new CodegenContext
    // Note: offset is 0 because baseOffset parameter already includes Platform.BYTE_ARRAY_OFFSET
    val offset = 0
    val getLong = "Platform.getLong"
    val putLong = "Platform.putLong"

    // Calculate the number of 64-bit words needed for null bit sets
    // Each word can hold null bits for up to 64 columns
    val bitset1Words = (schema1.size + 63) / 64 // Number of 8-byte words for row1's bitset
    val bitset2Words = (schema2.size + 63) / 64 // Number of 8-byte words for row2's bitset
    val outputBitsetWords = (schema1.size + schema2.size + 63) / 64 // Words needed for combined bitset
    val bitset1Remainder = schema1.size % 64 // How many bits of the last word in row1's bitset are used

    // Calculate memory savings from combining two bitsets into one.
    // When two rows are stored separately, their bitsets may have unused padding bits.
    // By combining them, we can potentially save one 8-byte word.
    // Example: If row1 has 65 columns (2 words, 63 bits unused) and row2 has 1 column (1 word, 63 bits unused),
    //          combined they need only 2 words instead of 3, saving 8 bytes.
    val sizeReduction = (bitset1Words + bitset2Words - outputBitsetWords) * 8

    // =====================================================================================
    // STEP 1: Generate code to copy and merge the null bit sets from both rows
    // =====================================================================================
    //
    // The null bit set tracks which columns are NULL. When joining two rows, we need to
    // concatenate their bit sets. This is complicated when row1's column count is not
    // a multiple of 64, because row2's bits need to be shifted to fill in the unused
    // portion of row1's last word.
    //
    // Example with bitset1Remainder = 3 (row1 has 3 columns, row2 has 2 columns):
    //
    //   Row1's bitset (1 word):  [bit0, bit1, bit2, 0, 0, 0, ..., 0]  (61 unused bits)
    //   Row2's bitset (1 word):  [bit0, bit1, 0, 0, ..., 0]          (62 unused bits)
    //
    //   Combined output (1 word): [r1.bit0, r1.bit1, r1.bit2, r2.bit0, r2.bit1, 0, ..., 0]
    //
    // The bit shifting handles this merging correctly.
    val copyBitset = Seq.tabulate(outputBitsetWords) { i =>
      val bits = if (bitset1Remainder > 0 && bitset2Words != 0) {
        // Non-aligned case: row1's column count is not a multiple of 64
        if (i < bitset1Words - 1) {
          // Copy row1's bitset words directly (except the last one)
          s"$getLong(obj1, offset1 + ${i * 8})"
        } else if (i == bitset1Words - 1) {
          // Merge row1's last word with row2's first word (shifted left)
          // row1's bits occupy the lower positions, row2's bits are shifted to fill the gap
          s"$getLong(obj1, offset1 + ${i * 8}) | ($getLong(obj2, offset2) << $bitset1Remainder)"
        } else if (i - bitset1Words < bitset2Words - 1) {
          // Middle words of row2: need to combine parts from two consecutive words
          // Lower bits come from the previous word (shifted right), upper bits from current word (shifted left)
          s"($getLong(obj2, offset2 + ${(i - bitset1Words) * 8}) >>> (64 - $bitset1Remainder))" +
            s" | ($getLong(obj2, offset2 + ${(i - bitset1Words + 1) * 8}) << $bitset1Remainder)"
        } else {
          // Last word of row2: only need the remaining bits shifted right
          s"$getLong(obj2, offset2 + ${(i - bitset1Words) * 8}) >>> (64 - $bitset1Remainder)"
        }
      } else {
        // Aligned case: row1's column count is a multiple of 64, or row2 is empty
        // No bit shifting needed - just copy words directly
        if (i < bitset1Words) {
          s"$getLong(obj1, offset1 + ${i * 8})"
        } else {
          s"$getLong(obj2, offset2 + ${(i - bitset1Words) * 8})"
        }
      }
      s"$putLong(baseObj, baseOffset + ${i * 8}, $bits);\n"
    }

    // Split the bitset copying code into multiple methods if it's too large
    // (Spark's CodeGenerator requires methods to stay under JVM bytecode limits)
    val copyBitsets = ctx.splitExpressions(
      expressions = copyBitset,
      funcName = "copyBitsetFunc",
      arguments = ("java.lang.Object", "obj1") :: ("long", "offset1") ::
        ("java.lang.Object", "obj2") :: ("long", "offset2") ::
        ("java.lang.Object", "baseObj") :: ("long", "baseOffset") :: Nil
    )

    // =====================================================================================
    // STEP 2: Generate code to copy fixed-length data from both rows
    // =====================================================================================
    //
    // Fixed-length section contains 8 bytes per column:
    // - For primitive types (int, long, double, etc.): the actual value
    // - For variable-length types (string, array, etc.): offset (32 bits) | size (32 bits)
    //
    // cursor tracks the current write position in the output buffer
    var cursor = offset + outputBitsetWords * 8
    val copyFixedLengthRow1 = s"""
                                 |// Copy fixed length data for row1 (${schema1.size} columns * 8 bytes)
                                 |Platform.copyMemory(
                                 |  obj1, offset1 + ${bitset1Words * 8},
                                 |  baseObj, baseOffset + $cursor,
                                 |  ${schema1.size * 8});
     """.stripMargin
    cursor += schema1.size * 8

    val copyFixedLengthRow2 = s"""
                                 |// Copy fixed length data for row2 (${schema2.size} columns * 8 bytes)
                                 |Platform.copyMemory(
                                 |  obj2, offset2 + ${bitset2Words * 8},
                                 |  baseObj, baseOffset + $cursor,
                                 |  ${schema2.size * 8});
     """.stripMargin
    cursor += schema2.size * 8

    // =====================================================================================
    // STEP 3: Generate code to copy variable-length data from both rows
    // =====================================================================================
    //
    // Variable-length data (strings, arrays, maps, structs) is stored after the fixed-length
    // section. The size is calculated as: totalRowSize - (bitsetSize + fixedLengthSize)
    //
    // Row1's variable data is copied first, then row2's variable data is appended.
    val numBytesBitsetAndFixedRow1 = (bitset1Words + schema1.size) * 8
    val copyVariableLengthRow1 = s"""
                                    |// Copy variable length data for row1
                                    |// Calculate size: total row bytes - (bitset bytes + fixed length bytes)
                                    |long numBytesVariableRow1 = row1.getSizeInBytes() - $numBytesBitsetAndFixedRow1;
                                    |Platform.copyMemory(
                                    |  obj1, offset1 + ${(bitset1Words + schema1.size) * 8},
                                    |  baseObj, baseOffset + $cursor,
                                    |  numBytesVariableRow1);
     """.stripMargin

    val numBytesBitsetAndFixedRow2 = (bitset2Words + schema2.size) * 8
    val copyVariableLengthRow2 = s"""
                                    |// Copy variable length data for row2 (appended after row1's variable data)
                                    |long numBytesVariableRow2 = row2.getSizeInBytes() - $numBytesBitsetAndFixedRow2;
                                    |Platform.copyMemory(
                                    |  obj2, offset2 + ${(bitset2Words + schema2.size) * 8},
                                    |  baseObj, baseOffset + $cursor + numBytesVariableRow1,
                                    |  numBytesVariableRow2);
     """.stripMargin

    // =====================================================================================
    // STEP 4: Generate code to update offsets for variable-length columns
    // =====================================================================================
    //
    // This is the most complex part. For variable-length columns (strings, arrays, etc.),
    // the fixed-length section stores: [offset (upper 32 bits) | size (lower 32 bits)]
    //
    // The offset is the byte position from the start of the UnsafeRow where the actual
    // data is stored. When we concatenate two rows, these offsets become invalid because:
    //
    // 1. The bitset size may have changed (due to merging)
    // 2. Row2's variable data is now after row1's variable data
    //
    // For columns from row1:
    //   newOffset = oldOffset + (outputBitsetWords - bitset1Words + schema2.size) * 8
    //   This accounts for: bitset size change + row2's fixed-length section inserted before
    //
    // For columns from row2:
    //   newOffset = oldOffset + (outputBitsetWords - bitset2Words + schema1.size) * 8 + row1VariableDataSize
    //   This accounts for: bitset size change + row1's fixed-length section + row1's variable data
    //
    val updateOffset = (schema1 ++ schema2).zipWithIndex.map { case (field, i) =>
      // Skip fixed length data types, and only generate code for variable length data
      if (UnsafeRow.isFixedLength(field.dataType)) {
        ""
      } else {
        // Calculate the shift amount for this column's offset
        // The offset is stored in the upper 32 bits, so we shift left by 32 when adding
        val shift =
          if (i < schema1.size) {
            // Column from row1: shift accounts for bitset change and row2's fixed-length section
            s"${(outputBitsetWords - bitset1Words + schema2.size) * 8}L"
          } else {
            // Column from row2: also need to account for row1's variable-length data
            s"(${(outputBitsetWords - bitset2Words + schema1.size) * 8}L + numBytesVariableRow1)"
          }
        val cursor = offset + outputBitsetWords * 8 + i * 8

        // IMPORTANT: Handling NULL values
        //
        // UnsafeRow stores offset=0 for NULL variable-length fields. We must NOT add the shift
        // to NULL fields, otherwise we'd create an invalid non-zero offset.
        //
        // We detect NULL fields by checking if existingOffset == 0. This works because:
        //
        // 1. For non-NULL fields with data: offset is always non-zero (points past the fixed section)
        // 2. For non-NULL empty strings/arrays: UnsafeRowWriter still stores a valid non-zero offset
        //    (the position where data would be, even though the size is 0)
        // 3. For NULL fields: offset is explicitly set to 0 by UnsafeRowWriter.setNullAt()
        //
        // This "offset == 0 means NULL" check is faster than reading the null bitmap.
        // Because UnsafeRowWriter.setNullAt() sets offset=0 and size=0 for NULLs,
        // we can rely on this property safely.
        s"""
           |existingOffset = $getLong(baseObj, baseOffset + $cursor);
           |if (existingOffset != 0) {
           |    // Add shift to upper 32 bits (offset) while preserving lower 32 bits (size)
           |    $putLong(baseObj, baseOffset + $cursor, existingOffset + ($shift << 32));
           |}
         """.stripMargin
      }
    }

    // Split the offset update code into multiple methods if needed
    val updateOffsets = ctx.splitExpressions(
      expressions = updateOffset,
      funcName = "updateOffsetsFunc",
      arguments = ("long", "numBytesVariableRow1") ::
        ("java.lang.Object", "baseObj") :: ("long", "baseOffset") :: Nil,
      makeSplitFunction = (s: String) => "long existingOffset;\n" + s
    )

    // =====================================================================================
    // STEP 5: Assemble the final generated code
    // =====================================================================================
    //
    // The generated class has this structure:
    // - A reusable UnsafeRow instance for output (avoids allocation per join)
    // - Helper methods for bitset copying and offset updates (if code was split)
    // - The main join() method that orchestrates all the steps
    //
    // The join() method:
    // 1. Calculates the output size: row1.size + row2.size - sizeReduction
    // 2. Gets base object and offset for both input rows
    // 3. Copies bitsets, fixed-length data, and variable-length data
    // 4. Updates offsets for variable-length columns
    // 5. Points the output UnsafeRow to the buffer and returns it
    val codeBody = s"""
       |public java.lang.Object generate(Object[] references) {
       |  return new SpecificUnsafeRowJoiner();
       |}
       |
       |class SpecificUnsafeRowJoiner extends ${classOf[CustomUnsafeRowJoiner].getName} {
       |  // Reusable output row to avoid allocation overhead during repeated joins
       |  private UnsafeRow out = new UnsafeRow(${schema1.size + schema2.size});
       |
       |  ${ctx.declareAddedFunctions()}
       |
       |  public UnsafeRow join(UnsafeRow row1, UnsafeRow row2, java.lang.Object baseObj, long baseOffset) {
       |    // Schema information for debugging:
       |    // row1: ${schema1.size} fields, $bitset1Words words in bitset
       |    // row2: ${schema2.size} fields, $bitset2Words words in bitset
       |    // output: ${schema1.size + schema2.size} fields, $outputBitsetWords words in bitset
       |
       |    // Calculate output size: sum of both rows minus bytes saved by merging bitsets
       |    final int sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes() - $sizeReduction;
       |
       |    // Get memory locations of input rows
       |    final java.lang.Object obj1 = row1.getBaseObject();
       |    final long offset1 = row1.getBaseOffset();
       |    final java.lang.Object obj2 = row2.getBaseObject();
       |    final long offset2 = row2.getBaseOffset();
       |
       |    // Step 1: Copy and merge null bitsets
       |    $copyBitsets
       |    // Step 2: Copy fixed-length sections
       |    $copyFixedLengthRow1
       |    $copyFixedLengthRow2
       |    // Step 3: Copy variable-length data
       |    $copyVariableLengthRow1
       |    $copyVariableLengthRow2
       |    // Step 4: Update offsets for variable-length columns
       |    long existingOffset;
       |    $updateOffsets
       |
       |    // Point output row to the buffer containing concatenated data (on-heap)
       |    out.pointTo(baseObj, baseOffset, sizeInBytes);
       |
       |    return out;
       |  }
       |}
     """.stripMargin
    // Format and compile the generated code
    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(codeBody, Map.empty))
    logDebug(s"SpecificUnsafeRowJoiner($schema1, $schema2):\n${CodeFormatter.format(code)}")

    // Compile the code and create an instance of the generated class
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(Array.empty).asInstanceOf[CustomUnsafeRowJoiner]
  }
}
