package indexeddataframe

import org.apache.spark.sql._
import org.scalatest.funsuite.AnyFunSuite
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators

class IndexedDFTest extends AnyFunSuite {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("spark test app")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.log.level", "ERROR")
    .getOrCreate()

  import sparkSession.implicits._
  sparkSession.experimental.extraStrategies ++= Seq(IndexedOperators)
  sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)

  test("createIndex") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    assert(idf.collect().length == df.collect().length)
  }

  test("createIndex(by name)") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex("src")

    assert(idf.collect().length == df.collect().length)
  }

  test("getRows") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    val rows = idf.getRows(1234)
    rows.show()

    assert(rows.collect().length == 2)
  }

  test("getRows (by name)") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex("src")

    val rows = idf.getRows(1234)
    rows.show()

    assert(rows.collect().length == 2)
  }

  test("filter") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()
    val idf = df.createIndex(0)
    idf.createOrReplaceTempView("idf")

    val rows = sparkSession.sql("SELECT * FROM idf WHERE src = 1234")
    rows.show()

    assert(rows.collect().length == 2)
  }

  test("filter (by name)") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag").cache()
    val idf = df.createIndex("src")
    idf.createOrReplaceTempView("idf_byname")

    val rows = sparkSession.sql("SELECT * FROM idf_byname WHERE src = 1234")
    rows.show()

    assert(rows.collect().length == 2)
  }

  test("join") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex(0).cache()

    myIDF.createOrReplaceTempView("indextable")
    df2.createOrReplaceTempView("nonindextable")

    val joinedDF = sparkSession.sql("select * from indextable join nonindextable on indextable.src = nonindextable.src")

    joinedDF.explain(true)

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

  test("join (by name)") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex("src").cache()

    myIDF.createOrReplaceTempView("indextable_byname")
    df2.createOrReplaceTempView("nonindextable_byname")

    val joinedDF = sparkSession.sql("select * from indextable_byname join nonindextable_byname on indextable_byname.src = nonindextable_byname.src")

    joinedDF.explain(true)

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

  test("join2") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex(1).cache()
    // val myIDF = myDf.cache()

    myIDF.createOrReplaceTempView("indextable")
    df2.createOrReplaceTempView("nonindextable")

    // Join on non indexed column. Should fallback to normal non-indexed joins.
    val joinedDF = sparkSession.sql("select * from indextable join nonindextable on indextable.src = nonindextable.src")

    joinedDF.explain(true)

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

  test("join2 (by name)") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef")).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex("dst").cache()
    // val myIDF = myDf.cache()

    myIDF.createOrReplaceTempView("indextable2_byname")
    df2.createOrReplaceTempView("nonindextable2_byname")

    // Join on non indexed column. Should fallback to normal non-indexed joins.
    val joinedDF =
      sparkSession.sql("select * from indextable2_byname join nonindextable2_byname on indextable2_byname.src = nonindextable2_byname.src")

    joinedDF.explain(true)

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

//  test("joinright") {
//
//    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag")
//    val df2 = Seq((1234, "test")).toDF("src", "data")
//
//    val myIDF = myDf.createIndex(0).cache()
//
//    myIDF.createOrReplaceTempView("indextable")
//    df2.createOrReplaceTempView("nonindextable")
//
//    val joinedDF = sparkSession.sql("select * from nonindextable join indextable on indextable.src = nonindextable.src")
//
//    joinedDF.explain(true)
//
//    joinedDF.show()
//    assert(joinedDF.collect().length == 2)
//  }

  test("string index") {
    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef"), (1, -3, "abcde")).toDF("src", "dst", "tag")
    val df = Seq(("abcd")).toDF("tag")

    val myIDF = myDf.createIndex(2).cache()

    val result = myIDF.getRows("abcde".asInstanceOf[AnyVal])

    assert(result.collect().length == 2)
  }

  test("string index (by name)") {
    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef"), (1, -3, "abcde")).toDF("src", "dst", "tag")
    val df = Seq(("abcd")).toDF("tag")

    val myIDF = myDf.createIndex("tag").cache()

    val result = myIDF.getRows("abcde".asInstanceOf[AnyVal])

    assert(result.collect().length == 2)
  }

  // ============================================
  // Multi-table join tests
  // ============================================

  test("three table chain join") {
    // Test: A JOIN B JOIN C where A is indexed
    val dfA = Seq((1, "a1"), (2, "a2"), (3, "a3")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (2, "b2"), (4, "b4")).toDF("id", "valB")
    val dfC = Seq((1, "c1"), (2, "c2"), (5, "c5")).toDF("id", "valC")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA")
    dfB.createOrReplaceTempView("tableB")
    dfC.createOrReplaceTempView("tableC")

    val result = sparkSession.sql("""
      SELECT * FROM tableA
      JOIN tableB ON tableA.id = tableB.id
      JOIN tableC ON tableA.id = tableC.id
    """)

    result.explain(true)
    result.show()

    // Only ids 1 and 2 are common to all three tables
    assert(result.collect().length == 2)
  }

  test("three table chain join - all indexed") {
    // Test: A JOIN B JOIN C where all tables are indexed
    val dfA = Seq((1, "a1"), (2, "a2"), (3, "a3")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (2, "b2"), (4, "b4")).toDF("id", "valB")
    val dfC = Seq((1, "c1"), (2, "c2"), (5, "c5")).toDF("id", "valC")

    val indexedA = dfA.createIndex(0).cache()
    val indexedB = dfB.createIndex(0).cache()
    val indexedC = dfC.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_all")
    indexedB.createOrReplaceTempView("tableB_all")
    indexedC.createOrReplaceTempView("tableC_all")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_all
      JOIN tableB_all ON tableA_all.id = tableB_all.id
      JOIN tableC_all ON tableA_all.id = tableC_all.id
    """)

    result.explain(true)
    result.show()

    // Only ids 1 and 2 are common to all three tables
    assert(result.collect().length == 2)
  }

  test("four table join") {
    // Test: A JOIN B JOIN C JOIN D
    val dfA = Seq((1, "a1"), (2, "a2"), (3, "a3")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (2, "b2")).toDF("id", "valB")
    val dfC = Seq((1, "c1"), (2, "c2"), (3, "c3")).toDF("id", "valC")
    val dfD = Seq((1, "d1"), (2, "d2")).toDF("id", "valD")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA4")
    dfB.createOrReplaceTempView("tableB4")
    dfC.createOrReplaceTempView("tableC4")
    dfD.createOrReplaceTempView("tableD4")

    val result = sparkSession.sql("""
      SELECT * FROM tableA4
      JOIN tableB4 ON tableA4.id = tableB4.id
      JOIN tableC4 ON tableA4.id = tableC4.id
      JOIN tableD4 ON tableA4.id = tableD4.id
    """)

    result.explain(true)
    result.show()

    // Only ids 1 and 2 are common to all four tables
    assert(result.collect().length == 2)
  }

  test("join with duplicate keys in multiple tables") {
    // Test: Joins with duplicate keys produce correct cartesian product
    val dfA = Seq((1, "a1"), (1, "a2"), (2, "a3")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (1, "b2"), (2, "b3")).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_dup")
    dfB.createOrReplaceTempView("tableB_dup")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_dup
      JOIN tableB_dup ON tableA_dup.id = tableB_dup.id
    """)

    result.show()

    // For id=1: 2 rows in A * 2 rows in B = 4 rows
    // For id=2: 1 row in A * 1 row in B = 1 row
    // Total: 5 rows
    assert(result.collect().length == 5)
  }

  test("join result with createIndex") {
    // Test: createIndex on join result
    val dfA = Seq((1, "a1", 100), (2, "a2", 200), (3, "a3", 300)).toDF("id", "valA", "score")
    val dfB = Seq((1, "b1"), (2, "b2")).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_nested")
    dfB.createOrReplaceTempView("tableB_nested")

    val joinedDF = sparkSession.sql("""
      SELECT tableA_nested.id, valA, score, valB
      FROM tableA_nested
      JOIN tableB_nested ON tableA_nested.id = tableB_nested.id
    """)

    // Create index on the join result
    val indexedJoined = joinedDF.createIndex(0).cache()

    indexedJoined.explain(true)
    indexedJoined.show()

    // Verify data integrity
    assert(indexedJoined.collect().length == 2)

    // Test getRows on the new index
    val rows = indexedJoined.getRows(1)
    assert(rows.collect().length == 1)
  }

  test("createIndex after filter") {
    // Test: createIndex on filtered DataFrame
    val df = Seq((1, 100), (2, 200), (3, 300), (4, 400), (5, 500)).toDF("id", "value")

    val filteredDF = df.filter("value >= 300")
    val indexed = filteredDF.createIndex(0).cache()

    indexed.show()

    assert(indexed.collect().length == 3)

    val rows = indexed.getRows(4)
    assert(rows.collect().length == 1)
  }

  test("createIndex after select with new columns") {
    // Test: createIndex on DataFrame with derived columns
    val df = Seq((1, 100), (2, 200), (3, 300)).toDF("id", "value")

    val derived = df.selectExpr("id", "value", "value * 2 as doubledValue")
    val indexed = derived.createIndex(0).cache()

    indexed.show()

    assert(indexed.collect().length == 3)

    val rows = indexed.getRows(2)
    val row = rows.collect()(0)
    assert(row.getInt(2) == 400) // doubledValue for id=2 should be 400
  }

  test("join with empty right table") {
    // Test: Join with empty table should return empty result
    val dfA = Seq((1, "a1"), (2, "a2")).toDF("id", "valA")
    val dfB = Seq.empty[(Int, String)].toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_empty")
    dfB.createOrReplaceTempView("tableB_empty")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_empty
      JOIN tableB_empty ON tableA_empty.id = tableB_empty.id
    """)

    result.show()

    assert(result.collect().length == 0)
  }

  test("join with no matching keys") {
    // Test: Join with no matching keys should return empty result
    val dfA = Seq((1, "a1"), (2, "a2")).toDF("id", "valA")
    val dfB = Seq((3, "b3"), (4, "b4")).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_nomatch")
    dfB.createOrReplaceTempView("tableB_nomatch")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_nomatch
      JOIN tableB_nomatch ON tableA_nomatch.id = tableB_nomatch.id
    """)

    result.show()

    assert(result.collect().length == 0)
  }

  test("large join with many partitions") {
    // Test: Join with data that spans multiple partitions
    val dataA = (1 to 1000).map(i => (i, s"a$i"))
    val dataB = (500 to 1500).map(i => (i, s"b$i"))

    val dfA = dataA.toDF("id", "valA")
    val dfB = dataB.toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_large")
    dfB.createOrReplaceTempView("tableB_large")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_large
      JOIN tableB_large ON tableA_large.id = tableB_large.id
    """)

    // Matching ids are 500 to 1000 (501 values)
    assert(result.collect().length == 501)
  }

  test("sequential joins reusing indexed table") {
    // Test: Multiple sequential joins using the same indexed table
    val dfA = Seq((1, "a1"), (2, "a2"), (3, "a3")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (2, "b2")).toDF("id", "valB")
    val dfC = Seq((2, "c2"), (3, "c3")).toDF("id", "valC")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_seq")
    dfB.createOrReplaceTempView("tableB_seq")
    dfC.createOrReplaceTempView("tableC_seq")

    // First join
    val result1 = sparkSession.sql("""
      SELECT tableA_seq.id, valA, valB
      FROM tableA_seq JOIN tableB_seq ON tableA_seq.id = tableB_seq.id
    """)
    assert(result1.collect().length == 2)

    // Second join using same indexed table
    val result2 = sparkSession.sql("""
      SELECT tableA_seq.id, valA, valC
      FROM tableA_seq JOIN tableC_seq ON tableA_seq.id = tableC_seq.id
    """)
    assert(result2.collect().length == 2)

    // Original indexed table should still work
    assert(indexedA.collect().length == 3)
  }

  test("join on different indexed columns") {
    // Test: Index on column A but join on column B (fallback to regular join)
    val dfA = Seq((1, 10, "a1"), (2, 20, "a2"), (3, 30, "a3")).toDF("id", "fk", "valA")
    val dfB = Seq((10, "b10"), (20, "b20"), (40, "b40")).toDF("fk", "valB")

    // Index A on id (column 0), but join on fk (column 1)
    // This should fallback to a regular join since join column != indexed column
    val indexedA = dfA.createIndex(0).cache()
    val indexedB = dfB.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_diffcol")
    indexedB.createOrReplaceTempView("tableB_diffcol")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_diffcol
      JOIN tableB_diffcol ON tableA_diffcol.fk = tableB_diffcol.fk
    """)

    result.explain(true)
    result.show()

    // fk values 10 and 20 match
    assert(result.collect().length == 2)
  }

  test("join using indexed column on right side") {
    // Test: Right table is indexed on the join column
    val dfA = Seq((10, "a10"), (20, "a20"), (30, "a30")).toDF("fk", "valA")
    val dfB = Seq((10, "b10"), (20, "b20"), (40, "b40")).toDF("fk", "valB")

    // Index B on fk (column 0), join on fk
    val indexedB = dfB.createIndex(0).cache()

    dfA.createOrReplaceTempView("tableA_rightidx")
    indexedB.createOrReplaceTempView("tableB_rightidx")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_rightidx
      JOIN tableB_rightidx ON tableA_rightidx.fk = tableB_rightidx.fk
    """)

    result.explain(true)
    result.show()

    // fk values 10 and 20 match
    assert(result.collect().length == 2)
  }

  test("mixed indexed and non-indexed multi-table join") {
    // Test: Some tables indexed, some not
    val dfA = Seq((1, "a1"), (2, "a2")).toDF("id", "valA")
    val dfB = Seq((1, "b1"), (2, "b2")).toDF("id", "valB")
    val dfC = Seq((1, "c1"), (2, "c2")).toDF("id", "valC")

    val indexedA = dfA.createIndex(0).cache()
    // B is not indexed
    val indexedC = dfC.createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_mixed")
    dfB.createOrReplaceTempView("tableB_mixed")
    indexedC.createOrReplaceTempView("tableC_mixed")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_mixed
      JOIN tableB_mixed ON tableA_mixed.id = tableB_mixed.id
      JOIN tableC_mixed ON tableA_mixed.id = tableC_mixed.id
    """)

    result.explain(true)
    result.show()

    assert(result.collect().length == 2)
  }

  test("self join on indexed table") {
    // Test: Self join on an indexed table
    val df = Seq((1, 2, "a"), (2, 3, "b"), (3, 1, "c")).toDF("id", "ref_id", "value")

    val indexed = df.createIndex(0).cache()

    indexed.createOrReplaceTempView("self_join_table")

    val result = sparkSession.sql("""
      SELECT t1.id, t1.value as val1, t2.value as val2
      FROM self_join_table t1
      JOIN self_join_table t2 ON t1.ref_id = t2.id
    """)

    result.show()

    // Each row references another row:
    // (1,2,a) -> id=2 exists -> match
    // (2,3,b) -> id=3 exists -> match
    // (3,1,c) -> id=1 exists -> match
    assert(result.collect().length == 3)
  }

  test("createIndex on already indexed then join") {
    // Test: createIndex on an indexed DataFrame (re-index)
    val df = Seq((1, 10, "a"), (2, 20, "b"), (3, 30, "c")).toDF("id", "fk", "value")
    val dfJoin = Seq((10, "x"), (20, "y")).toDF("fk", "extra")

    // First index on id
    val indexed1 = df.createIndex(0).cache()
    // Re-index on fk (column 1)
    val indexed2 = indexed1.createIndex(1).cache()

    indexed2.createOrReplaceTempView("reindexed_table")
    dfJoin.createOrReplaceTempView("join_fk_table")

    val result = sparkSession.sql("""
      SELECT * FROM reindexed_table
      JOIN join_fk_table ON reindexed_table.fk = join_fk_table.fk
    """)

    result.explain(true)
    result.show()

    // fk=10 and fk=20 match
    assert(result.collect().length == 2)
  }

  test("join with null values in key column") {
    // Test: Null values in join keys should not match
    val dfA = Seq((Some(1), "a1"), (Some(2), "a2"), (None, "a_null")).toDF("id", "valA")
    val dfB = Seq((Some(1), "b1"), (None, "b_null")).toDF("id", "valB")

    val indexedA = dfA.na.drop("any", Seq("id")).createIndex(0).cache()

    indexedA.createOrReplaceTempView("tableA_null")
    dfB.createOrReplaceTempView("tableB_null")

    val result = sparkSession.sql("""
      SELECT * FROM tableA_null
      JOIN tableB_null ON tableA_null.id = tableB_null.id
    """)

    result.show()

    // Only id=1 matches (nulls don't match in standard SQL join)
    assert(result.collect().length == 1)
  }

  // ============================================
  // Composite key join tests - potential bug verification
  // ============================================

  test("composite key join should work correctly") {
    // Test: Join with multiple equality conditions (composite key)
    // This tests if IndexedShuffledEquiJoinExec correctly handles composite keys
    // BUG: Currently only the first key is used, ignoring subsequent conditions
    val dfA = Seq(
      (1, 10, "a1"),
      (1, 20, "a2"),
      (2, 10, "a3"),
      (2, 20, "a4")
    ).toDF("key1", "key2", "valA")

    val dfB = Seq(
      (1, 10, "b1"), // Should match (1, 10, "a1")
      (1, 30, "b2"), // Should NOT match (key2 doesn't match)
      (2, 20, "b3") // Should match (2, 20, "a4")
    ).toDF("key1", "key2", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("composite_a")
    dfB.createOrReplaceTempView("composite_b")

    // Composite key join: key1 AND key2 must both match
    val result = sparkSession.sql("""
      SELECT * FROM composite_a
      JOIN composite_b ON composite_a.key1 = composite_b.key1
                      AND composite_a.key2 = composite_b.key2
    """)

    result.explain(true)
    result.show()

    // Expected: only 2 rows should match (where BOTH key1 AND key2 match)
    // BUG: If only key1 is used, we would get 4 rows instead
    // (1,10) matches (1,10) -> 1 row
    // (2,20) matches (2,20) -> 1 row
    // Total: 2 rows
    val count = result.collect().length
    println(s"Composite key join result count: $count (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count - composite key join may be ignoring secondary key conditions")
  }

  test("composite key join - both sides indexed") {
    // Test: Both sides are indexed, composite key join
    val dfA = Seq(
      (1, 10, "a1"),
      (1, 20, "a2"),
      (2, 10, "a3")
    ).toDF("key1", "key2", "valA")

    val dfB = Seq(
      (1, 10, "b1"),
      (1, 20, "b2"),
      (2, 30, "b3")
    ).toDF("key1", "key2", "valB")

    val indexedA = dfA.createIndex(0).cache()
    val indexedB = dfB.createIndex(0).cache()

    indexedA.createOrReplaceTempView("composite_both_a")
    indexedB.createOrReplaceTempView("composite_both_b")

    val result = sparkSession.sql("""
      SELECT * FROM composite_both_a
      JOIN composite_both_b ON composite_both_a.key1 = composite_both_b.key1
                           AND composite_both_a.key2 = composite_both_b.key2
    """)

    result.explain(true)
    result.show()

    // Expected: 2 rows (1,10) and (1,20) match
    val count = result.collect().length
    println(s"Composite key join (both indexed) result count: $count (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("triple composite key join") {
    // Test: Join with three key columns
    val dfA = Seq(
      (1, 10, 100, "a1"),
      (1, 10, 200, "a2"),
      (1, 20, 100, "a3")
    ).toDF("k1", "k2", "k3", "valA")

    val dfB = Seq(
      (1, 10, 100, "b1"), // Matches a1
      (1, 10, 300, "b2"), // No match (k3 differs)
      (1, 20, 200, "b3") // No match (k3 differs from a3)
    ).toDF("k1", "k2", "k3", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("triple_a")
    dfB.createOrReplaceTempView("triple_b")

    val result = sparkSession.sql("""
      SELECT * FROM triple_a
      JOIN triple_b ON triple_a.k1 = triple_b.k1
                   AND triple_a.k2 = triple_b.k2
                   AND triple_a.k3 = triple_b.k3
    """)

    result.explain(true)
    result.show()

    // Only (1, 10, 100) matches
    val count = result.collect().length
    println(s"Triple composite key join result count: $count (expected: 1)")
    assert(count == 1, s"Expected 1 row but got $count")
  }

  // ============================================
  // Semi-Join and Outer-Join tests
  // These tests verify whether IndexedShuffledEquiJoinExec
  // correctly handles different join types or produces wrong results
  // ============================================

  test("left semi join - indexed left") {
    // LEFT SEMI JOIN returns rows from left table that have matching rows in right table
    // Output should only contain left table columns
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2"),
      (4, "b4") // id=4 doesn't exist in A
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("semi_left_a")
    dfB.createOrReplaceTempView("semi_left_b")

    val result = sparkSession.sql("""
      SELECT * FROM semi_left_a
      WHERE EXISTS (SELECT 1 FROM semi_left_b WHERE semi_left_a.id = semi_left_b.id)
    """)

    result.explain(true)
    println("=== Left Semi Join (EXISTS) Result ===")
    result.show()

    // Expected: rows with id=1 and id=2 from A (2 rows)
    // Output columns should be only from A: (id, valA)
    val count = result.collect().length
    val cols = result.columns.length
    println(s"Left Semi Join result: $count rows, $cols columns (expected: 2 rows, 2 columns)")
    assert(count == 2, s"Expected 2 rows but got $count")
    assert(cols == 2, s"Expected 2 columns (only from left) but got $cols")
  }

  test("left semi join - SQL syntax") {
    // Explicit LEFT SEMI JOIN syntax
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("semi_sql_a")
    dfB.createOrReplaceTempView("semi_sql_b")

    val result = sparkSession.sql("""
      SELECT * FROM semi_sql_a LEFT SEMI JOIN semi_sql_b ON semi_sql_a.id = semi_sql_b.id
    """)

    result.explain(true)
    println("=== Left Semi Join (SQL) Result ===")
    result.show()

    val count = result.collect().length
    val cols = result.columns.length
    println(s"Left Semi Join (SQL) result: $count rows, $cols columns (expected: 2 rows, 2 columns)")
    assert(count == 2, s"Expected 2 rows but got $count")
    assert(cols == 2, s"Expected 2 columns but got $cols")
  }

  test("left anti join - indexed left") {
    // LEFT ANTI JOIN returns rows from left table that have NO matching rows in right table
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("anti_a")
    dfB.createOrReplaceTempView("anti_b")

    val result = sparkSession.sql("""
      SELECT * FROM anti_a
      WHERE NOT EXISTS (SELECT 1 FROM anti_b WHERE anti_a.id = anti_b.id)
    """)

    result.explain(true)
    println("=== Left Anti Join Result ===")
    result.show()

    // Expected: only id=3 from A (1 row)
    val count = result.collect().length
    println(s"Left Anti Join result: $count rows (expected: 1)")
    assert(count == 1, s"Expected 1 row but got $count")
  }

  test("left anti join - SQL syntax") {
    // Explicit LEFT ANTI JOIN syntax
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("anti_sql_a")
    dfB.createOrReplaceTempView("anti_sql_b")

    val result = sparkSession.sql("""
      SELECT * FROM anti_sql_a LEFT ANTI JOIN anti_sql_b ON anti_sql_a.id = anti_sql_b.id
    """)

    result.explain(true)
    println("=== Left Anti Join (SQL) Result ===")
    result.show()

    val count = result.collect().length
    println(s"Left Anti Join (SQL) result: $count rows (expected: 1)")
    assert(count == 1, s"Expected 1 row but got $count")
  }

  test("left outer join - indexed left") {
    // LEFT OUTER JOIN returns all rows from left, with nulls for non-matching right rows
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2"),
      (4, "b4")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("left_outer_a")
    dfB.createOrReplaceTempView("left_outer_b")

    val result = sparkSession.sql("""
      SELECT * FROM left_outer_a
      LEFT OUTER JOIN left_outer_b ON left_outer_a.id = left_outer_b.id
    """)

    result.explain(true)
    println("=== Left Outer Join Result ===")
    result.show()

    // Expected: 3 rows (all from A)
    // id=1: (1, a1, 1, b1)
    // id=2: (2, a2, 2, b2)
    // id=3: (3, a3, null, null)
    val collected = result.collect()
    val count = collected.length
    println(s"Left Outer Join result: $count rows (expected: 3)")
    assert(count == 3, s"Expected 3 rows but got $count")

    // Check that id=3 has null values for right side
    val row3 = collected.find(_.getInt(0) == 3)
    assert(row3.isDefined, "Row with id=3 should exist")
    assert(row3.get.isNullAt(2) || row3.get.isNullAt(3), "Right side columns should be null for id=3")
  }

  test("right outer join - indexed left") {
    // RIGHT OUTER JOIN returns all rows from right, with nulls for non-matching left rows
    val dfA = Seq(
      (1, "a1"),
      (2, "a2")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2"),
      (3, "b3")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("right_outer_a")
    dfB.createOrReplaceTempView("right_outer_b")

    val result = sparkSession.sql("""
      SELECT * FROM right_outer_a
      RIGHT OUTER JOIN right_outer_b ON right_outer_a.id = right_outer_b.id
    """)

    result.explain(true)
    println("=== Right Outer Join Result ===")
    result.show()

    // Expected: 3 rows (all from B)
    // id=1: (1, a1, 1, b1)
    // id=2: (2, a2, 2, b2)
    // id=3: (null, null, 3, b3)
    val collected = result.collect()
    val count = collected.length
    println(s"Right Outer Join result: $count rows (expected: 3)")
    assert(count == 3, s"Expected 3 rows but got $count")
  }

  test("full outer join - indexed left") {
    // FULL OUTER JOIN returns all rows from both tables
    val dfA = Seq(
      (1, "a1"),
      (2, "a2")
    ).toDF("id", "valA")

    val dfB = Seq(
      (2, "b2"),
      (3, "b3")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("full_outer_a")
    dfB.createOrReplaceTempView("full_outer_b")

    val result = sparkSession.sql("""
      SELECT * FROM full_outer_a
      FULL OUTER JOIN full_outer_b ON full_outer_a.id = full_outer_b.id
    """)

    result.explain(true)
    println("=== Full Outer Join Result ===")
    result.show()

    // Expected: 3 rows
    // id=1: (1, a1, null, null)
    // id=2: (2, a2, 2, b2)
    // id=3: (null, null, 3, b3)
    val count = result.collect().length
    println(s"Full Outer Join result: $count rows (expected: 3)")
    assert(count == 3, s"Expected 3 rows but got $count")
  }

  test("left outer join - indexed right") {
    // LEFT OUTER JOIN where the right side is indexed
    val dfA = Seq(
      (1, "a1"),
      (2, "a2"),
      (3, "a3")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2")
    ).toDF("id", "valB")

    val indexedB = dfB.createIndex(0).cache()

    dfA.createOrReplaceTempView("left_outer_right_idx_a")
    indexedB.createOrReplaceTempView("left_outer_right_idx_b")

    val result = sparkSession.sql("""
      SELECT * FROM left_outer_right_idx_a
      LEFT OUTER JOIN left_outer_right_idx_b ON left_outer_right_idx_a.id = left_outer_right_idx_b.id
    """)

    result.explain(true)
    println("=== Left Outer Join (Right Indexed) Result ===")
    result.show()

    // Expected: 3 rows
    val count = result.collect().length
    println(s"Left Outer Join (Right Indexed) result: $count rows (expected: 3)")
    assert(count == 3, s"Expected 3 rows but got $count")
  }

  test("right outer join - indexed right") {
    // RIGHT OUTER JOIN where the right side is indexed
    val dfA = Seq(
      (1, "a1"),
      (2, "a2")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (2, "b2"),
      (3, "b3")
    ).toDF("id", "valB")

    val indexedB = dfB.createIndex(0).cache()

    dfA.createOrReplaceTempView("right_outer_right_idx_a")
    indexedB.createOrReplaceTempView("right_outer_right_idx_b")

    val result = sparkSession.sql("""
      SELECT * FROM right_outer_right_idx_a
      RIGHT OUTER JOIN right_outer_right_idx_b ON right_outer_right_idx_a.id = right_outer_right_idx_b.id
    """)

    result.explain(true)
    println("=== Right Outer Join (Right Indexed) Result ===")
    result.show()

    // Expected: 3 rows
    val count = result.collect().length
    println(s"Right Outer Join (Right Indexed) result: $count rows (expected: 3)")
    assert(count == 3, s"Expected 3 rows but got $count")
  }

  test("left semi join with duplicates") {
    // LEFT SEMI JOIN should not produce duplicates even if right has multiple matches
    val dfA = Seq(
      (1, "a1"),
      (2, "a2")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (1, "b1_dup"), // Duplicate key in right
      (2, "b2")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("semi_dup_a")
    dfB.createOrReplaceTempView("semi_dup_b")

    val result = sparkSession.sql("""
      SELECT * FROM semi_dup_a LEFT SEMI JOIN semi_dup_b ON semi_dup_a.id = semi_dup_b.id
    """)

    result.explain(true)
    println("=== Left Semi Join with Duplicates Result ===")
    result.show()

    // Expected: 2 rows (no duplicates from semi join)
    val count = result.collect().length
    println(s"Left Semi Join with Duplicates result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count - semi join should not produce duplicates")
  }

  test("cross join - should not use indexed join") {
    // CROSS JOIN should not use IndexedJoin
    val dfA = Seq((1, "a1"), (2, "a2")).toDF("id", "valA")
    val dfB = Seq((10, "b1"), (20, "b2")).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("cross_a")
    dfB.createOrReplaceTempView("cross_b")

    val result = sparkSession.sql("""
      SELECT * FROM cross_a CROSS JOIN cross_b
    """)

    result.explain(true)
    println("=== Cross Join Result ===")
    result.show()

    // Expected: 2 * 2 = 4 rows (cartesian product)
    val count = result.collect().length
    println(s"Cross Join result: $count rows (expected: 4)")
    assert(count == 4, s"Expected 4 rows but got $count")
  }

  test("left outer join with duplicates on both sides") {
    // Test LEFT OUTER JOIN with duplicate keys
    val dfA = Seq(
      (1, "a1"),
      (1, "a1_dup"),
      (2, "a2")
    ).toDF("id", "valA")

    val dfB = Seq(
      (1, "b1"),
      (1, "b1_dup"),
      (3, "b3")
    ).toDF("id", "valB")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("left_outer_dup_a")
    dfB.createOrReplaceTempView("left_outer_dup_b")

    val result = sparkSession.sql("""
      SELECT * FROM left_outer_dup_a
      LEFT OUTER JOIN left_outer_dup_b ON left_outer_dup_a.id = left_outer_dup_b.id
    """)

    result.explain(true)
    println("=== Left Outer Join with Duplicates Result ===")
    result.show()

    // Expected:
    // id=1: 2 rows in A * 2 rows in B = 4 rows
    // id=2: 1 row in A * 0 rows in B = 1 row (with nulls)
    // Total: 5 rows
    val count = result.collect().length
    println(s"Left Outer Join with Duplicates result: $count rows (expected: 5)")
    assert(count == 5, s"Expected 5 rows but got $count")
  }

  // ============================================
  // Non-equi join predicate tests
  // Tests for otherPredicates support in IndexedShuffledEquiJoinExec
  // ============================================

  test("join with non-equi predicate - greater than") {
    // Test: equi-join + non-equi predicate (a.value > b.threshold)
    val dfA = Seq(
      (1, 100),
      (2, 200),
      (3, 300),
      (4, 50)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 80), // id=1, threshold=80 -> value(100) > threshold(80) = true
      (2, 250), // id=2, threshold=250 -> value(200) > threshold(250) = false
      (3, 300), // id=3, threshold=300 -> value(300) > threshold(300) = false
      (4, 40) // id=4, threshold=40 -> value(50) > threshold(40) = true
    ).toDF("id", "threshold")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_gt_a")
    dfB.createOrReplaceTempView("nonequi_gt_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_gt_a a
      JOIN nonequi_gt_b b ON a.id = b.id AND a.value > b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (>) Result ===")
    result.show()

    // Expected: only id=1 and id=4 satisfy both equi (id match) and non-equi (value > threshold)
    val count = result.collect().length
    println(s"Join with > predicate result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count - non-equi predicate may not be applied")

    // Verify the correct rows are returned
    val ids = result.collect().map(_.getInt(0)).sorted
    assert(ids.sameElements(Array(1, 4)), s"Expected ids [1, 4] but got ${ids.mkString("[", ", ", "]")}")
  }

  test("join with non-equi predicate - less than") {
    // Test: equi-join + non-equi predicate (a.value < b.threshold)
    val dfA = Seq(
      (1, 100),
      (2, 200),
      (3, 50)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 150), // value(100) < threshold(150) = true
      (2, 150), // value(200) < threshold(150) = false
      (3, 100) // value(50) < threshold(100) = true
    ).toDF("id", "threshold")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_lt_a")
    dfB.createOrReplaceTempView("nonequi_lt_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_lt_a a
      JOIN nonequi_lt_b b ON a.id = b.id AND a.value < b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (<) Result ===")
    result.show()

    // Expected: id=1 and id=3
    val count = result.collect().length
    println(s"Join with < predicate result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("join with non-equi predicate - greater than or equal") {
    // Test: a.value >= b.threshold
    val dfA = Seq(
      (1, 100),
      (2, 200),
      (3, 300)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 100), // value(100) >= threshold(100) = true
      (2, 250), // value(200) >= threshold(250) = false
      (3, 200) // value(300) >= threshold(200) = true
    ).toDF("id", "threshold")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_gte_a")
    dfB.createOrReplaceTempView("nonequi_gte_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_gte_a a
      JOIN nonequi_gte_b b ON a.id = b.id AND a.value >= b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (>=) Result ===")
    result.show()

    // Expected: id=1 and id=3
    val count = result.collect().length
    println(s"Join with >= predicate result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("join with non-equi predicate - not equal") {
    // Test: a.status != b.status
    val dfA = Seq(
      (1, "active"),
      (2, "inactive"),
      (3, "active")
    ).toDF("id", "status")

    val dfB = Seq(
      (1, "active"), // status match -> != is false
      (2, "active"), // status differ -> != is true
      (3, "pending") // status differ -> != is true
    ).toDF("id", "status")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_neq_a")
    dfB.createOrReplaceTempView("nonequi_neq_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_neq_a a
      JOIN nonequi_neq_b b ON a.id = b.id AND a.status != b.status
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (!=) Result ===")
    result.show()

    // Expected: id=2 and id=3 (where status differs)
    val count = result.collect().length
    println(s"Join with != predicate result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("join with multiple non-equi predicates") {
    // Test: equi-join + multiple non-equi predicates
    val dfA = Seq(
      (1, 100, 10),
      (2, 200, 20),
      (3, 300, 30),
      (4, 150, 15)
    ).toDF("id", "value", "score")

    val dfB = Seq(
      (1, 80, 5), // value(100) > 80 AND score(10) > 5 -> true AND true = true
      (2, 180, 25), // value(200) > 180 AND score(20) > 25 -> true AND false = false
      (3, 350, 20), // value(300) > 350 -> false
      (4, 100, 10) // value(150) > 100 AND score(15) > 10 -> true AND true = true
    ).toDF("id", "min_value", "min_score")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_multi_a")
    dfB.createOrReplaceTempView("nonequi_multi_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_multi_a a
      JOIN nonequi_multi_b b ON a.id = b.id
                            AND a.value > b.min_value
                            AND a.score > b.min_score
    """)

    result.explain(true)
    println("=== Join with Multiple Non-Equi Predicates Result ===")
    result.show()

    // Expected: id=1 and id=4 (both conditions must be true)
    val count = result.collect().length
    println(s"Join with multiple non-equi predicates result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")

    val ids = result.collect().map(_.getInt(0)).sorted
    assert(ids.sameElements(Array(1, 4)), s"Expected ids [1, 4] but got ${ids.mkString("[", ", ", "]")}")
  }

  test("join with non-equi predicate and duplicates") {
    // Test: non-equi predicate with duplicate keys
    val dfA = Seq(
      (1, 100),
      (1, 200), // duplicate key with different value
      (2, 150)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 150), // For id=1: value(100) > 150 = false, value(200) > 150 = true
      (2, 100) // For id=2: value(150) > 100 = true
    ).toDF("id", "threshold")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_dup_a")
    dfB.createOrReplaceTempView("nonequi_dup_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_dup_a a
      JOIN nonequi_dup_b b ON a.id = b.id AND a.value > b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate and Duplicates Result ===")
    result.show()

    // Expected: (1, 200, 1, 150) and (2, 150, 2, 100) = 2 rows
    val count = result.collect().length
    println(s"Join with non-equi + duplicates result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("join with non-equi predicate - right side indexed") {
    // Test: non-equi predicate when right side is indexed
    val dfA = Seq(
      (1, 100),
      (2, 200),
      (3, 50)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 80), // value(100) > 80 = true
      (2, 250), // value(200) > 250 = false
      (3, 40) // value(50) > 40 = true
    ).toDF("id", "threshold")

    val indexedB = dfB.createIndex(0).cache()

    dfA.createOrReplaceTempView("nonequi_right_a")
    indexedB.createOrReplaceTempView("nonequi_right_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_right_a a
      JOIN nonequi_right_b b ON a.id = b.id AND a.value > b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (Right Indexed) Result ===")
    result.show()

    // Expected: id=1 and id=3
    val count = result.collect().length
    println(s"Join with non-equi (right indexed) result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }

  test("join with non-equi predicate - no matches") {
    // Test: non-equi predicate filters out all equi-join matches
    val dfA = Seq(
      (1, 10),
      (2, 20)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 100), // value(10) > 100 = false
      (2, 200) // value(20) > 200 = false
    ).toDF("id", "threshold")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_nomatch_a")
    dfB.createOrReplaceTempView("nonequi_nomatch_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_nomatch_a a
      JOIN nonequi_nomatch_b b ON a.id = b.id AND a.value > b.threshold
    """)

    result.explain(true)
    println("=== Join with Non-Equi Predicate (No Matches) Result ===")
    result.show()

    // Expected: 0 rows (all filtered by non-equi predicate)
    val count = result.collect().length
    println(s"Join with non-equi (no matches) result: $count rows (expected: 0)")
    assert(count == 0, s"Expected 0 rows but got $count")
  }

  test("join with BETWEEN-like non-equi predicate") {
    // Test: a.value BETWEEN b.min_val AND b.max_val
    val dfA = Seq(
      (1, 50),
      (2, 150),
      (3, 250)
    ).toDF("id", "value")

    val dfB = Seq(
      (1, 0, 100), // value(50) between 0 and 100 = true
      (2, 100, 200), // value(150) between 100 and 200 = true
      (3, 300, 400) // value(250) between 300 and 400 = false
    ).toDF("id", "min_val", "max_val")

    val indexedA = dfA.createIndex(0).cache()

    indexedA.createOrReplaceTempView("nonequi_between_a")
    dfB.createOrReplaceTempView("nonequi_between_b")

    val result = sparkSession.sql("""
      SELECT * FROM nonequi_between_a a
      JOIN nonequi_between_b b ON a.id = b.id
                              AND a.value >= b.min_val
                              AND a.value <= b.max_val
    """)

    result.explain(true)
    println("=== Join with BETWEEN-like Non-Equi Predicate Result ===")
    result.show()

    // Expected: id=1 and id=2
    val count = result.collect().length
    println(s"Join with BETWEEN-like predicate result: $count rows (expected: 2)")
    assert(count == 2, s"Expected 2 rows but got $count")
  }
}
