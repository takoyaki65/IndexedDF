package indexeddataframe

import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import indexeddataframe.execution._
import indexeddataframe.logical._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys

/** strategies for the operators applied on indexed dataframes
  */
object IndexedOperators extends SparkStrategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateIndex(colNo, child) => CreateIndexExec(colNo, planLater(child)) :: Nil
    /** this is a strategy for eliminating the [InMemoryRelation] that spark generates when .cache() is called on an ordinary dataframe; in that case,
      * the representation of the data frame is changed to a CachedBatch; we cannot have that on the indexed data frames as we would lose the indexing
      * capabilities; therefore, we just insert a dummy strategy that returns an operator which works on "indexed RDDs"
      */
    case IndexedBlockRDD(output, rdd, child: IndexedOperatorExec) =>
      val indexedChild = child.asInstanceOf[IndexedOperatorExec]
      IndexedBlockRDDScanExec(output, rdd, indexedChild.indexColNo, rdd.partitionsRDD.getNumPartitions) :: Nil

    case GetRows(key, child) => GetRowsExec(key, planLater(child)) :: Nil
    /** dummy filter object for the moment; in the future, we will implement filtering functionality on the indexed data
      */
    case IndexedFilter(condition, child) => IndexedFilterExec(condition, planLater(child)) :: Nil
    case IndexedJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition, JoinHint.NONE) match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherPredicates, conditionOnJoinKeys, lChild, rChild, _) =>
          // compute the index of the left side keys == column number
          // Use semanticEquals to compare attributes as they may have different metadata
          val leftColNo = lChild.output.indexWhere(_.semanticEquals(leftKeys.head))
          // compute the index of the right side keys == column number
          val rightColNo = rChild.output.indexWhere(_.semanticEquals(rightKeys.head))

          // Pass otherPredicates (non-equi join conditions like a.value > b.threshold)
          // to the physical operator for post-join filtering
          // otherPredicates is Option[Expression], convert to Seq[Expression]
          IndexedShuffledEquiJoinExec(
            planLater(left),
            planLater(right),
            joinType,
            leftColNo,
            rightColNo,
            otherPredicates.toSeq,
            conditionOnJoinKeys
          ) :: Nil
        case _ => Nil
      }
    case _ => Nil
  }
}
