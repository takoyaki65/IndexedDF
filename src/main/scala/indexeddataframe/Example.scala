package indexeddataframe

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.{DataFrame, SparkSession}
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators
import org.apache.spark.sql.types._

/** IndexedDFのデモプログラム 実行: sbt "runMain indexeddataframe.Example"
  */
object Example {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("IndexedDF Example")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import sparkSession.implicits._

    // 戦略と最適化ルールを登録
    sparkSession.experimental.extraStrategies ++= Seq(IndexedOperators)
    sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)

    println("=== IndexedDF Demo ===\n")

    // サンプルデータ作成
    val edges = (1 to 10000).map(i => (i.toLong, (i * 7 % 1000).toLong, s"edge_$i"))
    val edgesDF = edges.toDF("src", "dst", "label").cache()
    edgesDF.count() // trigger cache

    val nodes = (1 to 1000).map(i => (i.toLong, s"node_$i"))
    val nodesDF = nodes.toDF("id", "name").cache()
    nodesDF.count() // trigger cache

    println(s"Edges: ${edgesDF.count()} rows")
    println(s"Nodes: ${nodesDF.count()} rows\n")

    // 1. インデックス作成
    println("--- 1. Create Index ---")
    val t1 = System.nanoTime()
    val indexedDF = edgesDF.createIndex(0).cache()
    // trigger execution
    indexedDF.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeIndexed().count()
    val t2 = System.nanoTime()
    println(s"Index creation: ${(t2 - t1) / 1000000.0} ms\n")

    // 2. キールックアップ
    println("--- 2. Key Lookup (getRows) ---")
    val lookupKey = 100L
    val t3 = System.nanoTime()
    val result = indexedDF.getRows(lookupKey)
    val rows = result.collect()
    val t4 = System.nanoTime()
    println(s"Lookup key=$lookupKey: found ${rows.length} rows in ${(t4 - t3) / 1000000.0} ms")
    rows.foreach(r => println(s"  $r"))
    println()

    // 3. フィルタ (インデックス活用)
    println("--- 3. Indexed Filter ---")
    indexedDF.createOrReplaceTempView("indexed_edges")
    val filterResult = sparkSession.sql("SELECT * FROM indexed_edges WHERE src = 500")
    filterResult.explain(true)
    println(s"Filter result: ${filterResult.count()} rows\n")

    // 4. Join (インデックス活用)
    println("--- 4. Indexed Join ---")
    nodesDF.createOrReplaceTempView("nodes")
    val joinResult = sparkSession.sql("""SELECT e.src, e.dst, e.label, n.name
        |FROM indexed_edges e
        |JOIN nodes n ON e.src = n.id
        |""".stripMargin)
    joinResult.explain(true)

    val t5 = System.nanoTime()
    val joinCount = joinResult.count()
    val t6 = System.nanoTime()
    println(s"Join result: $joinCount rows in ${(t6 - t5) / 1000000.0} ms")
    joinResult.show(10)

    println("=== Demo Complete ===")
    sparkSession.stop()
  }
}

/** ベンチマークプログラム
  *
  * 外部CSVファイルを使用する場合: sbt "runMain indexeddataframe.BenchmarkPrograms <delimiter1> <path1> <delimiter2> <path2> <partitions> <master>"
  *
  * インメモリデータでデモ実行する場合: sbt "runMain indexeddataframe.BenchmarkPrograms"
  */
object BenchmarkPrograms {

  val nTimesRun = 10

  def triggerExecutionDF(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.execute()
    plan.foreachPartition(p => println("partition size = " + p.length))
  }

  def triggerExecutionDF2(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.execute()
    plan.foreachPartition(p => println("final part size = " + p.length))
  }

  def triggerExecutionIndexedDF(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeIndexed()
    plan.foreachPartition(p => println("indexed part size = " + p.length))
  }

  def createIndexAndCache(df: DataFrame): DataFrame = {
    val t1 = System.nanoTime()
    val indexed = df.createIndex(0).cache()
    triggerExecutionIndexedDF(indexed)
    val t2 = System.nanoTime()
    println("Index Creation took %f ms".format((t2 - t1) / 1000000.0))
    indexed
  }

  def runJoin(indexedDF: DataFrame, nodesDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")
    nodesDF.createOrReplaceTempView("vertices")

    val res = sparkSession.sql(
      "SELECT * " +
        "FROM edges " +
        "JOIN vertices " +
        "ON edges.src = vertices.id"
    )

    res.explain(true)

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF2(res)
      val t2 = System.nanoTime()
      println("join iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("join on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def runScan(indexedDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges")

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF(res)
      val t2 = System.nanoTime()
      println("scan iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("scan on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def runFilter(indexedDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges where edges.src > 5000")
    res.explain(true)

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF(res)
      val t2 = System.nanoTime()
      println("filter iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("filter on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def runEqFilter(indexedDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges where edges.src = 5000")
    res.explain(true)

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF(res)
      val t2 = System.nanoTime()
      println("filter iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("equality filter on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def runAgg(indexedDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT sum(src) FROM edges")

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF(res)
      val t2 = System.nanoTime()
      println("agg iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("agg on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def runProj(indexedDF: DataFrame, sparkSession: SparkSession): Unit = {
    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT dst FROM edges")

    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()
      triggerExecutionDF(res)
      val t2 = System.nanoTime()
      println("proj iteration %d took %f".format(i, (t2 - t1) / 1000000.0))
      if (i > 1) totalTime += (t2 - t1)
    }
    println("proj on Indexed DataFrame took %f ms".format(totalTime / ((nTimesRun - 1) * 1000000.0)))
  }

  def getSizeInBytes(df: DataFrame): Unit = {
    // df.queryExecution.optimizedPlan.statistics.sizeInBytes
  }

  def main(args: Array[String]): Unit = {
    // 引数がない場合はインメモリデータでデモ実行
    if (args.length < 6) {
      runWithSampleData()
      return
    }

    // 外部CSVファイルを使用
    val delimiter1 = args(0)
    val path1 = args(1)
    val delimiter2 = args(2)
    val path2 = args(3)
    val partitions = args(4)
    val master = args(5)

    val sparkSession = SparkSession
      .builder()
      .master(master)
      .appName("IndexedDF Benchmark")
      .config("spark.sql.shuffle.partitions", partitions)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.locality.wait", "200")
      .config("spark.shuffle.reduceLocality.enabled", "false")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.experimental.extraStrategies ++= Seq(IndexedOperators)
    sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)

    val edgeSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("cost", FloatType, nullable = true)
      )
    )
    val nodeSchema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("firstName", StringType, nullable = true),
        StructField("lastName", StringType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("birthday", DateType, nullable = true),
        StructField("creationDate", StringType, nullable = true),
        StructField("locationIP", StringType, nullable = true),
        StructField("browserUsed", StringType, nullable = true)
      )
    )

    var edgesDF = sparkSession.read
      .option("header", "true")
      .option("delimiter", delimiter1)
      .schema(edgeSchema)
      .csv(path1)

    var nodesDF = sparkSession.read
      .option("header", "true")
      .option("delimiter", delimiter2)
      .schema(nodeSchema)
      .csv(path2)

    nodesDF = nodesDF.cache()
    edgesDF = edgesDF.cache()
    triggerExecutionDF(nodesDF)
    triggerExecutionDF(edgesDF)

    val indexedDF = createIndexAndCache(edgesDF)

    runJoin(indexedDF, nodesDF, sparkSession)

    indexedDF.show(10)

    runScan(indexedDF, sparkSession)
    runFilter(indexedDF, sparkSession)
    runEqFilter(indexedDF, sparkSession)
    runAgg(indexedDF, sparkSession)
    runProj(indexedDF, sparkSession)

    sparkSession.close()
    sparkSession.stop()
  }

  /** インメモリデータでベンチマーク実行（外部ファイル不要）
    */
  def runWithSampleData(): Unit = {
    println("Running with in-memory sample data...")
    println()

    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("IndexedDF Benchmark (Sample Data)")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.experimental.extraStrategies ++= Seq(IndexedOperators)
    sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)

    // サンプルデータ作成
    val edges = (1 to 10000).map(i => (i.toLong, (i * 7 % 1000).toLong, 1.0f))
    var edgesDF = edges.toDF("src", "dst", "cost").cache()

    val nodes = (1 to 1000).map(i => (i.toLong, s"name_$i"))
    var nodesDF = nodes.toDF("id", "name").cache()

    triggerExecutionDF(nodesDF)
    triggerExecutionDF(edgesDF)

    println(s"Edges: ${edgesDF.count()} rows")
    println(s"Nodes: ${nodesDF.count()} rows")
    println()

    val indexedDF = createIndexAndCache(edgesDF)

    println("\n=== Running benchmarks ===\n")

    runScan(indexedDF, sparkSession)
    println()

    runFilter(indexedDF, sparkSession)
    println()

    runEqFilter(indexedDF, sparkSession)
    println()

    runAgg(indexedDF, sparkSession)
    println()

    runProj(indexedDF, sparkSession)
    println()

    runJoin(indexedDF, nodesDF, sparkSession)
    println()

    indexedDF.show(10)

    sparkSession.close()
    sparkSession.stop()
  }
}
