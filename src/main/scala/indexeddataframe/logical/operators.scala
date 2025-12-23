package indexeddataframe.logical

import indexeddataframe.IRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.util.LongAccumulator

/**
 * Logical operators for Indexed DataFrame operations.
 *
 * == Overview ==
 *
 * This file defines the logical plan nodes for Indexed DataFrame operations.
 * These operators integrate with Spark's Catalyst optimizer and are transformed
 * into physical operators by [[indexeddataframe.IndexedOperators]] strategy.
 *
 * == Operator Hierarchy ==
 *
 * {{{
 *                      LogicalPlan (Spark)
 *                            │
 *                    IndexedOperator (trait)
 *                            │
 *        ┌───────────────────┼───────────────────┐
 *        │                   │                   │
 *   UnaryNode           LeafNode            BinaryNode
 *        │                   │                   │
 *   ┌────┴────┐        ┌─────┴─────┐            │
 *   │         │        │           │            │
 * CreateIndex │  IndexedLocal  IndexedBlock  IndexedJoin
 *           GetRows  Relation      RDD
 *             │
 *       IndexedFilter
 * }}}
 *
 * == Query Plan Transformation ==
 *
 * The operators are used in the following pipeline:
 * {{{
 *   User API (createIndex, getRows, etc.)
 *         │
 *         ▼
 *   Logical Operators (this file)
 *         │
 *         ▼
 *   ConvertToIndexedOperators (optimization rule)
 *         │
 *         ▼
 *   IndexedOperators Strategy (physical planning)
 *         │
 *         ▼
 *   Physical Operators (execution/operators.scala)
 * }}}
 */

// =============================================================================
// Base Trait for Indexed Operators
// =============================================================================

/**
 * Base trait for all indexed operators in the logical plan.
 *
 * This trait provides common functionality for indexed operators:
 * - Prevents Catalyst from dropping input columns via references override
 * - Provides isIndexed check to verify if the plan tree contains indexed data
 *
 * == Column Preservation ==
 *
 * Catalyst's optimizer may drop columns that appear unused in the final output.
 * However, indexed operators need all input columns for index lookups and joins.
 * By overriding `references` to return `inputSet`, we ensure all columns are preserved.
 *
 * == Index Detection ==
 *
 * The `isIndexed` method recursively checks if any child plan contains indexed data.
 * This is used by optimization rules to determine if index-based operations can be applied.
 */
trait IndexedOperator extends LogicalPlan {

  /**
   * Returns all input attributes as references to prevent column pruning.
   *
   * Every indexed operator relies on its input having a specific set of columns,
   * so we override references to include all inputs to prevent Catalyst
   * from dropping any input columns.
   */
  override def references: AttributeSet = inputSet

  /**
   * Checks if this plan tree contains indexed data.
   *
   * Recursively searches child plans for IndexedOperator nodes.
   *
   * @return true if any child contains indexed data
   * 
   * @note It isn't used. should be removed?
   */
  def isIndexed: Boolean = children.exists(_.find {
    case p: IndexedOperator => p.isIndexed
    case _                  => false
  }.nonEmpty)

}

// =============================================================================
// Index Creation Operator
// =============================================================================

/**
 * Logical operator for creating an index on a DataFrame column.
 *
 * This operator wraps the child plan and specifies which column to index.
 * When executed, it triggers the creation of an [[InternalIndexedPartition]]
 * for each partition of the input data.
 *
 * == Usage ==
 *
 * {{{
 *   // Via implicits
 *   val indexedDF = df.createIndex(0)  // Index on column 0
 *
 *   // Internal logical plan structure
 *   CreateIndex(colNo = 0, child = originalPlan)
 * }}}
 *
 * @param colNo 0-based index of the column to build the index on
 * @param child The child logical plan whose output will be indexed
 */
case class CreateIndex(val colNo: Int, child: LogicalPlan) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): CreateIndex = copy(child = newChild)
}

// =============================================================================
// Key Lookup Operator
// =============================================================================

/**
 * Logical operator for retrieving rows by a specific key value.
 *
 * This operator performs a point lookup on the indexed DataFrame,
 * returning all rows where the indexed column matches the given key.
 * Duplicate keys are supported - all matching rows are returned.
 *
 * == Usage ==
 *
 * {{{
 *   // Via implicits
 *   val rows = indexedDF.getRows(1234L)
 *
 *   // Internal logical plan structure
 *   GetRows(key = 1234L, child = indexedPlan)
 * }}}
 *
 * @param key   The key value to look up (supports Long, Int, String, Double)
 * @param child The child logical plan (must be an indexed operator)
 */
case class GetRows(val key: AnyVal, child: LogicalPlan) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): GetRows =
    copy(child = newChild)
}

// =============================================================================
// Leaf Nodes (Data Sources)
// =============================================================================

/**
 * A local (in-driver) relation containing indexed data.
 *
 * This is analogous to Spark's LocalRelation but for indexed data.
 * Used primarily for testing and small datasets that fit in driver memory.
 *
 * == MultiInstanceRelation ==
 *
 * Implements MultiInstanceRelation to support self-joins and other patterns
 * where the same relation appears multiple times in a query. Each instance
 * gets fresh expression IDs to avoid attribute conflicts.
 * 
 * @note It isn't used. should be removed?
 *
 * @param output Schema of the relation (must be fully resolved)
 * @param data   The actual row data stored in driver memory
 */
case class IndexedLocalRelation(output: Seq[Attribute], data: Seq[InternalRow]) extends LeafNode with MultiInstanceRelation with IndexedOperator {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
   * Creates a copy of this relation with new expression IDs.
   *
   * Required when a relation is included multiple times in the same query
   * (e.g., self-joins) to avoid attribute ID conflicts.
   */
  override final def newInstance(): this.type = {
    IndexedLocalRelation(output.map(_.newInstance()), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)
}

/**
 * A distributed relation backed by an indexed RDD.
 *
 * This is the primary representation of indexed data in the logical plan.
 * It wraps an [[IRDD]] (RDD of InternalIndexedPartition) and provides
 * the schema information needed for query planning.
 *
 * == Lifecycle ==
 *
 * {{{
 *   1. User calls df.createIndex(colNo).cache()
 *   2. CreateIndexExec builds InternalIndexedPartition for each partition
 *   3. Result is wrapped in IRDD and stored in IndexedBlockRDD
 *   4. Subsequent operations reference this IndexedBlockRDD
 * }}}
 *
 * == Note on Statistics ==
 *
 * Statistics computation is currently disabled (commented out) because
 * accessing RDD metrics during logical planning can cause issues.
 * Future work may enable statistics for better query optimization.
 *
 * @param output Schema of the indexed relation
 * @param rdd    The underlying indexed RDD containing InternalIndexedPartition data
 * @param child  The original SparkPlan used to create this indexed data
 */
case class IndexedBlockRDD(output: Seq[Attribute], rdd: IRDD, child: SparkPlan) extends IndexedOperator with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  /**
   * Creates a copy of this relation with new expression IDs.
   *
   * Required for queries that reference the same indexed data multiple times.
   */
  override def newInstance(): IndexedBlockRDD.this.type =
    IndexedBlockRDD(output.map(_.newInstance()), rdd, child).asInstanceOf[this.type]

  /** All output attributes are produced by this operator (no pass-through) */
  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]
  ): IndexedBlockRDD = this

  /*
  override lazy val statistics: Statistics = {
    val batchStats: LongAccumulator = child.sqlContext.sparkContext.longAccumulator
    /*if (batchStats.value == 0L) {
      Statistics(sizeInBytes = org.apache.spark.sql.internal.SQLConf.DEFAULT_SIZE_IN_BYTES.defaultValue.get)
    } else {
      Statistics(sizeInBytes = batchStats.value.longValue)
    }*/
    Statistics(0)
  }
   */
}

// =============================================================================
// Join Operator
// =============================================================================

/**
 * Logical operator for indexed equi-join.
 *
 * This operator represents a join where at least one side is indexed.
 * The index is used to accelerate the join by performing index lookups
 * instead of a full shuffle-based join.
 *
 * == Join Execution Strategy ==
 *
 * The physical planning strategy selects between two implementations:
 * - [[IndexedBroadcastEquiJoinExec]]: When non-indexed side is small enough to broadcast
 * - [[IndexedShuffledEquiJoinExec]]: When both sides are large (shuffle-based)
 *
 * == Supported Join Types ==
 *
 * Currently only INNER JOIN is supported. Other join types (LEFT OUTER, etc.)
 * fall back to standard Spark join implementations.
 *
 * == Query Plan Transformation ==
 *
 * {{{
 *   Standard Join (detected by ConvertToIndexedOperators)
 *            │
 *            ▼
 *   IndexedJoin (if one side is indexed)
 *            │
 *            ▼
 *   IndexedBroadcastEquiJoinExec or IndexedShuffledEquiJoinExec
 * }}}
 *
 * @param left      Left side of the join (may or may not be indexed)
 * @param right     Right side of the join (may or may not be indexed)
 * @param joinType  Type of join (only INNER currently supported for indexed execution)
 * @param condition Optional join condition (equi-join condition)
 */
case class IndexedJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression])
    extends BinaryNode
    with IndexedOperator {

  /** Output is the concatenation of left and right outputs */
  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan
  ): IndexedJoin =
    copy(left = newLeft, right = newRight)
}

// =============================================================================
// Filter Operator
// =============================================================================

/**
 * Logical operator for filtering indexed data.
 *
 * This operator applies a filter condition to indexed data.
 * When the filter condition is an equality check on the indexed column,
 * it can be optimized to use the index for O(log n) lookup.
 *
 * == Optimization ==
 *
 * The [[ConvertToIndexedOperators]] rule detects filter conditions of the form:
 * {{{
 *   indexed_column = literal_value
 * }}}
 *
 * And converts them to use index lookups via [[GetRows]] or [[IndexedFilterExec]].
 *
 * @param condition The filter predicate expression
 * @param child     The indexed child plan to filter
 */
case class IndexedFilter(condition: Expression, child: IndexedOperator) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): IndexedFilter =
    copy(child = newChild.asInstanceOf[IndexedOperator])
}
