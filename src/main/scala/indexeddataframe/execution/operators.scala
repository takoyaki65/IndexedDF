package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, EqualTo, Expression, Literal}
import indexeddataframe.{IRDD, InternalIndexedPartition, Utils}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

trait IndexedOperatorExec extends SparkPlan {
  private val logger = LoggerFactory.getLogger(classOf[IndexedOperatorExec])

  // override def outputPartitioning: Partitioning = child.outputPartitioning
  def executeIndexed(): IRDD

  // the number of the indexed column
  def indexColNo = 0

  /** if the indexed operator is required to return rows (i.e., as for a regular spark DF operations) produce its rows by scanning the index
    */
  override def doExecute() = {
    executeIndexed()
  }

  override def executeCollect(): Array[InternalRow] = {
    logger.debug("executing the collect operator")
    executeIndexed().collect()
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    logger.debug("executing the take operator")
    if (n == 0) {
      return new Array[InternalRow](0)
    }
    executeIndexed().take(n)
  }

  def executeGetRows(key: AnyVal): Array[InternalRow] = {
    val resultRDD = executeIndexed().get(key)
    resultRDD.collect()
  }

  def executeMultiGetRows(keys: Array[AnyVal]): Array[InternalRow] = {
    val resultRDD = executeIndexed().multiget(keys)
    resultRDD.collect()
  }
}

/** physical operator that creates the index
  * @param indexColNo
  * @param child
  */
case class CreateIndexExec(override val indexColNo: Int, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  private val logger = LoggerFactory.getLogger(classOf[CreateIndexExec])

  override def output: Seq[Attribute] = child.output

  // we need to repartition when creating the Index in order to know how to partition the appends and join probes
  override def outputPartitioning = HashPartitioning(Seq(child.output(indexColNo)), conf.numShufflePartitions)
  override def requiredChildDistribution: Seq[Distribution] = Seq(ClusteredDistribution(Seq(child.output(indexColNo))))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): CreateIndexExec =
    copy(child = newChildren.head)

  override def executeIndexed(): IRDD = {
    logger.debug("executing the createIndex operator")

    // create the index
    val childRDD = child.execute()
    val rddId = childRDD.id
    val outputAttrs = output

    val partitions = childRDD
      .mapPartitionsWithIndex[InternalIndexedPartition](
        (partitionId, rowIter) => Iterator(Utils.doIndexing(indexColNo, rowIter, outputAttrs, rddId, partitionId)),
        preservesPartitioning = true
      )
    val ret = new IRDD(indexColNo, partitions)
    Utils.ensureCached(ret)
  }
}

/** a physical operator that is used to replace the InMemoryRelation of default Spark, as InMemoryRelation stores CachedBatches, while in our indexed
  * DataFrame we do not want to change the representation, we just "cache" the data structure as is
  * @param output
  * @param rdd
  * @param child
  */
case class IndexedBlockRDDScanExec(output: Seq[Attribute], rdd: IRDD, override val indexColNo: Int, numPartitions: Int)
    extends LeafExecNode
    with IndexedOperatorExec {
  private val logger = LoggerFactory.getLogger(classOf[IndexedBlockRDDScanExec])

  // Declare the output partitioning as hash partitioned on the index column
  // Tell spark not to shuffle after this operator
  override def outputPartitioning: Partitioning = HashPartitioning(Seq(output(indexColNo)), numPartitions)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): IndexedBlockRDDScanExec = this

  override def executeIndexed(): IRDD = {
    logger.debug("executing the cache() operator")

    Utils.ensureCached(rdd)
  }
}

/** operator that performs key lookups
  * @param key
  * @param child
  */
case class GetRowsExec(key: AnyVal, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): GetRowsExec =
    copy(child = newChildren.head)

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val resultRDD = rdd.get(key)

    resultRDD
  }
}

/** dummy filter object, does not do anything atm will be used in the future for applying filter on the indexed DataFrame
  * @param condition
  * @param child
  */
case class IndexedFilterExec(condition: Expression, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): IndexedFilterExec =
    copy(child = newChildren.head)

  override def doExecute(): RDD[InternalRow] = {
    condition match {
      case EqualTo(_, literalValue: Literal) => {
        val key = literalValue.value.asInstanceOf[AnyVal]
        val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
        val resultRDD = rdd.get(key)

        resultRDD
      }
      case _ => null
    }
  }
}

/**
 * Physical operator for shuffled equi-join between an indexed DataFrame and a regular DataFrame.
 *
 * This operator performs an equi-join using the index for efficient lookups, then applies
 * any additional non-equi predicates (e.g., `a.value > b.threshold`) to filter the results.
 *
 * @param left            The indexed DataFrame (left side of join)
 * @param right           A regular DataFrame (right side of join)
 * @param leftCol         Column index for join key on the left table
 * @param rightCol        Column index for join key on the right table
 * @param otherPredicates Non-equi join predicates to apply after the equi-join
 */
case class IndexedShuffledEquiJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftCol: Int,
    rightCol: Int,
    otherPredicates: Seq[Expression] = Seq.empty
) extends BinaryExecNode {
  private val logger = LoggerFactory.getLogger(classOf[IndexedShuffledEquiJoinExec])

  override def output: Seq[Attribute] = left.output ++ right.output

  // Use child's output attributes directly to ensure exprId matches with child's outputPartitioning.
  // This prevents Spark's EnsureRequirements from inserting unnecessary ShuffleExchangeExec
  // when the indexed side is already hash-partitioned on the join column.
  override def requiredChildDistribution: Seq[Distribution] =
    Seq(ClusteredDistribution(Seq(left.output(leftCol))), ClusteredDistribution(Seq(right.output(rightCol))))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): IndexedShuffledEquiJoinExec =
    copy(left = newChildren(0), right = newChildren(1))

  /**
   * Applies non-equi predicates to filter joined rows.
   *
   * Binds the predicate expressions to the output schema and evaluates them
   * for each row. Only rows satisfying all predicates are returned.
   */
  private def applyOtherPredicates(result: RDD[InternalRow]): RDD[InternalRow] = {
    if (otherPredicates.isEmpty) {
      result
    } else {
      val outputAttrs = output
      val predicates = otherPredicates
      result.mapPartitions { iter =>
        // Bind predicates to output schema inside the partition
        val boundPredicates = predicates.map { pred =>
          BindReferences.bindReference(pred, outputAttrs)
        }
        iter.filter { row =>
          boundPredicates.forall(_.eval(row).asInstanceOf[Boolean])
        }
      }
    }
  }

  // helper method to find the IndexedOperatorExec in the plan tree
  private def findIndexedOp(plan: SparkPlan): Option[IndexedOperatorExec] = {
    plan match {
      case indexed: IndexedOperatorExec => Some(indexed)
      case _                            => None
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    logger.debug("in the Shuffled JOIN operator")

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    // check which side is indexed AND whether join column matches the indexed column
    val leftIndexedOp = findIndexedOp(left)
    val rightIndexedOp = findIndexedOp(right)

    // Only use the indexed side if the join column matches the indexed column
    val useLeftIndex = leftIndexedOp.exists(op => op.indexColNo == leftCol)
    val useRightIndex = rightIndexedOp.exists(op => op.indexColNo == rightCol)

    val equiJoinResult = (useLeftIndex, useRightIndex, leftIndexedOp, rightIndexedOp) match {
      case (true, _, Some(indexedLeft), _) =>
        val leftRDD = indexedLeft.executeIndexed()
        val rightRDD = right.execute()

        leftRDD.partitionsRDD.zipPartitions(rightRDD, true) { (leftIter, rightIter) =>
          // generate an unsafe row joiner
          // Note: InternalIndexedPartition.get() returns rows WITHOUT prev column
          val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
          if (leftIter.hasNext) {
            leftIter.next().multigetJoinedRight(rightIter, joiner, right.output, rightCol)
          } else Iterator.empty
        }

      case (_, true, _, Some(indexedRight)) =>
        val leftRDD = left.execute()
        val rightRDD = indexedRight.executeIndexed()

        rightRDD.partitionsRDD.zipPartitions(leftRDD, true) { (rightIter, leftIter) =>
          // generate an unsafe row joiner
          // Note: InternalIndexedPartition.get() returns rows WITHOUT prev column
          val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
          if (rightIter.hasNext) {
            rightIter.next().multigetJoinedLeft(leftIter, joiner, left.output, leftCol)
          } else Iterator.empty
        }

      case _ =>
        throw new IllegalStateException(
          s"IndexedShuffleEquiJoinExec requires at least one indexed child where join column matches index column. " +
            s"leftCol=$leftCol, rightCol=$rightCol, leftIndexColNo=${leftIndexedOp.map(_.indexColNo)}, rightIndexColNo=${rightIndexedOp.map(_.indexColNo)}"
        )
    }

    // Apply non-equi predicates (e.g., a.value > b.threshold) after the equi-join
    applyOtherPredicates(equiJoinResult)
  }

}

/**
 * Physical operator for broadcast equi-join between an indexed DataFrame and a regular DataFrame.
 *
 * This operator broadcasts the smaller (right) side to all partitions and uses the index
 * on the left side for efficient lookups. Any additional non-equi predicates are applied
 * after the equi-join.
 *
 * @param left            The indexed DataFrame (left side of join)
 * @param right           A regular DataFrame to be broadcast (right side of join)
 * @param leftCol         Column index for join key on the left table
 * @param rightCol        Column index for join key on the right table
 * @param otherPredicates Non-equi join predicates to apply after the equi-join
 */
case class IndexedBroadcastEquiJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftCol: Int,
    rightCol: Int,
    otherPredicates: Seq[Expression] = Seq.empty
) extends BinaryExecNode {
  private val logger = LoggerFactory.getLogger(classOf[IndexedBroadcastEquiJoinExec])

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]
  ): IndexedBroadcastEquiJoinExec =
    copy(left = newChildren(0), right = newChildren(1))

  /**
   * Applies non-equi predicates to filter joined rows.
   */
  private def applyOtherPredicates(result: RDD[InternalRow]): RDD[InternalRow] = {
    if (otherPredicates.isEmpty) {
      result
    } else {
      val outputAttrs = output
      val predicates = otherPredicates
      result.mapPartitions { iter =>
        val boundPredicates = predicates.map { pred =>
          BindReferences.bindReference(pred, outputAttrs)
        }
        iter.filter { row =>
          boundPredicates.forall(_.eval(row).asInstanceOf[Boolean])
        }
      }
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    logger.debug("in the Broadcast JOIN operator")

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val t1 = System.nanoTime()
    // broadcast the right relation
    val rightRDD = sparkContext.broadcast(right.executeCollect())
    val t2 = System.nanoTime()

    logger.debug("collect + broadcast time = %f".format((t2 - t1) / 1000000.0))

    val equiJoinResult = leftRDD.multigetBroadcast(rightRDD, leftSchema, rightSchema, right.output, rightCol)

    // Apply non-equi predicates after the equi-join
    applyOtherPredicates(equiJoinResult)
  }
}
