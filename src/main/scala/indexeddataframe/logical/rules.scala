package indexeddataframe.logical

import indexeddataframe.execution.IndexedOperatorExec
import indexeddataframe.{IRDD, Utils}
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, IsNotNull}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.storage.StorageLevel

/**
 * Catalyst optimization rules for Indexed DataFrame operations.
 *
 * == Overview ==
 *
 * This file contains optimization rules that transform standard Spark logical plans
 * into indexed operations when applicable. These rules are registered via:
 * {{{
 *   sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)
 * }}}
 *
 * == Rule Execution Order ==
 *
 * {{{
 *   Standard LogicalPlan (Join, Filter, etc.)
 *            │
 *            ▼
 *   ConvertToIndexedOperators (this file)
 *            │
 *            ├─ Detects InMemoryRelation with IndexedOperatorExec
 *            │  └─▶ Converts to IndexedBlockRDD
 *            │
 *            ├─ Detects Join on indexed column
 *            │  └─▶ Converts to IndexedJoin
 *            │
 *            └─ Detects Filter on indexed column
 *               └─▶ Converts to IndexedFilter
 * }}}
 *
 * == Cache Management ==
 *
 * The rules maintain a cache (via TrieMap) of already-executed indexed plans
 * to avoid redundant computation when the same indexed data is accessed multiple times.
 */

// =============================================================================
// Local Relation Indexing Rule
// =============================================================================

/**
 * Rule for indexing local (in-driver) relations.
 *
 * Transforms CreateIndex on a LocalRelation into an IndexedLocalRelation.
 * This is primarily used for testing with small datasets.
 *
 * == Transformation ==
 *
 * {{{
 *   CreateIndex(colNo, LocalRelation(output, data))
 *            │
 *            ▼
 *   IndexedLocalRelation(output, data)
 * }}}
 * 
 * @note It isn't used. should be removed?
 */
object IndexLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform { case CreateIndex(colNo, lr: LocalRelation) =>
    IndexedLocalRelation(lr.output, lr.data)
  }
}

// =============================================================================
// Main Optimization Rule
// =============================================================================

/**
 * Main optimization rule for converting standard Spark operators to indexed operators.
 *
 * This rule is the core of the Indexed DataFrame optimization. It detects patterns
 * in the logical plan that can benefit from indexing and transforms them accordingly.
 *
 * == Supported Transformations ==
 *
 * 1. '''Cache Detection''': Converts InMemoryRelation with indexed child to IndexedBlockRDD
 * 2. '''Indexed Join''': Converts equi-join on indexed column to IndexedJoin (INNER only)
 * 3. '''Indexed Filter''': Converts equality filter on indexed column to IndexedFilter
 * 4. '''IsNotNull Elimination''': Removes redundant IsNotNull checks on indexed data
 *
 * == Limitations ==
 *
 * - Only INNER JOIN is supported; other join types fall back to Spark's default
 * - Only single-column indexes are supported; composite keys are not supported
 * - Filter optimization only applies to equality conditions (not range queries)
 *
 * == Registration ==
 *
 * {{{
 *   sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)
 * }}}
 */
object ConvertToIndexedOperators extends Rule[LogicalPlan] {

  // ===========================================================================
  // Cache Management
  // ===========================================================================

  /**
   * Cache for already-executed indexed plans.
   *
   * Similar to Spark SQL's CacheManager, this tracks which indexed data has been
   * materialized to avoid redundant computation. Uses CTrie for thread-safety.
   *
   * Key: The SparkPlan that produced the indexed data
   * Value: The resulting IRDD (RDD of InternalIndexedPartition)
   */
  private val cachedPlan: TrieMap[SparkPlan, IRDD] = new TrieMap[SparkPlan, IRDD]

  /**
   * Retrieves cached indexed data or executes and caches the plan.
   *
   * This implements a simple cache-aside pattern:
   * 1. Check if the plan is already cached
   * 2. If not, execute the plan and cache the result
   * 3. Return the cached IRDD
   *
   * @param plan The physical plan to execute or retrieve from cache
   * @return The IRDD containing indexed partition data
   */
  private def getIfCached(plan: SparkPlan, storageLevel: StorageLevel): IRDD = {
    val result = cachedPlan.get(plan)
    if (result == None) {
      val executedPlan = Utils.ensureCached(plan.asInstanceOf[IndexedOperatorExec].executeIndexed(), storageLevel)
      cachedPlan.put(plan, executedPlan)
      executedPlan
    } else {
      result.get
    }
  }

  // ===========================================================================
  // Index Detection Utilities
  // ===========================================================================

  /**
   * Checks if a logical plan contains indexed operators.
   *
   * @param plan The logical plan to check
   * @return true if any node in the plan tree is an IndexedOperator
   * 
   * @note This function is not used anywhere. Consider removing it.
   */
  def isIndexed(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: IndexedOperator => true
      case _                  => false
    }.nonEmpty
  }

  /**
   * Checks if a physical plan contains indexed operators.
   *
   * @param plan The physical plan to check
   * @return true if any node in the plan tree is an IndexedOperatorExec
   * 
   * @note This function is not used anywhere. Consider removing it.
   */
  def isIndexed(plan: SparkPlan): Boolean = {
    plan.find {
      case _: IndexedOperatorExec => true
      case _                      => false
    }.nonEmpty
  }

  // ===========================================================================
  // Projection-through-IndexedBlockRDD Extractor
  // ===========================================================================

  /**
   * Extractor object for detecting IndexedBlockRDD through optional Project layers.
   *
   * This enables indexed join optimizations even when Catalyst's ColumnPruning
   * rule has inserted a Project between the IndexedBlockRDD and the Join.
   *
   * == Problem ==
   *
   * For queries like `SELECT COUNT(*) FROM indexed_table JOIN other ON ...`,
   * Catalyst's ColumnPruning optimization inserts a Project to drop unused columns:
   * {{{
   *   Join
   *   ├── Project [id]        <-- Inserted by ColumnPruning
   *   │   └── IndexedBlockRDD
   *   └── other
   * }}}
   *
   * This breaks the pattern match `case Join(left: IndexedBlockRDD, ...)`.
   *
   * == Solution ==
   *
   * This extractor looks through Project layers to find the underlying IndexedBlockRDD:
   * {{{
   *   case ExtractIndexedBlockRDD(indexedRDD, Some(project)) => // Found through Project
   *   case ExtractIndexedBlockRDD(indexedRDD, None) => // Direct IndexedBlockRDD
   * }}}
   */
  object ExtractIndexedBlockRDD {

    /**
     * Extracts IndexedBlockRDD from a logical plan, looking through Project layers.
     *
     * @param plan The logical plan to extract from
     * @return Some((IndexedBlockRDD, Option[Project])) if found, None otherwise.
     *         The Option[Project] contains the outermost Project if present.
     */
    def unapply(plan: LogicalPlan): Option[(IndexedBlockRDD, Option[Project])] = plan match {
      case indexed: IndexedBlockRDD                           => Some((indexed, None))
      case project @ Project(_, child: IndexedBlockRDD)       => Some((child, Some(project)))
      case project @ Project(_, innerProject @ Project(_, _)) =>
        // Recursively handle nested Projects (rare but possible)
        unapply(innerProject) match {
          case Some((indexed, _)) => Some((indexed, Some(project)))
          case None               => None
        }
      case _ => None
    }
  }

  // ===========================================================================
  // Join Condition Validation
  // ===========================================================================

  /**
   * Checks if a join can use the left side's index.
   *
   * For a join to use the left side's index:
   * 1. The left side must be an IndexedBlockRDD
   * 2. The join must be an equi-join on exactly one column
   * 3. The join column must be the indexed column
   *
   * == Example ==
   *
   * {{{
   *   -- This can use indexed join (assuming 'id' is indexed on left)
   *   SELECT * FROM indexed_table JOIN other ON indexed_table.id = other.id
   *
   *   -- This CANNOT use indexed join (composite key)
   *   SELECT * FROM indexed_table JOIN other
   *     ON indexed_table.id = other.id AND indexed_table.name = other.name
   * }}}
   *
   * @param left      The left side of the join (must be IndexedBlockRDD)
   * @param right     The right side of the join
   * @param joinType  The type of join
   * @param condition The join condition
   * @return true if the join can use the left side's index
   */
  def joiningIndexedColumnLeft(left: IndexedBlockRDD, right: LogicalPlan, joinType: JoinType, condition: Option[Expression]): Boolean = {
    Join(left, right, joinType, condition, JoinHint.NONE) match {
      case ExtractEquiJoinKeys(_, leftKeys, _, _, _, lChild, _, _) =>
        // Only support single-key joins; composite keys are not supported by IndexedJoin
        if (leftKeys.length != 1) return false
        val leftColNo = lChild.output.indexWhere(_.semanticEquals(leftKeys.head))
        leftColNo == left.rdd.colNo
      case _ => false
    }
  }

  /**
   * Checks if a join can use the left side's index when Project is present.
   *
   * Similar to [[joiningIndexedColumnLeft]] but handles cases where a Project
   * sits between the IndexedBlockRDD and the Join.
   *
   * @param left         The left side of the join (Project over IndexedBlockRDD)
   * @param indexedChild The underlying IndexedBlockRDD
   * @param right        The right side of the join
   * @param joinType     The type of join
   * @param condition    The join condition
   * @return true if the join can use the index
   */
  def joiningIndexedColumnLeftThroughProject(
      left: LogicalPlan,
      indexedChild: IndexedBlockRDD,
      right: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression]
  ): Boolean = {
    Join(left, right, joinType, condition, JoinHint.NONE) match {
      case ExtractEquiJoinKeys(_, leftKeys, _, _, _, lChild, _, _) =>
        if (leftKeys.length != 1) return false
        // The join key must be in the Project's output AND match the indexed column
        val leftKey = leftKeys.head
        // Find which column in the original IndexedBlockRDD the join key maps to
        val indexedColNo = indexedChild.rdd.colNo
        val indexedAttr = indexedChild.output(indexedColNo)
        // Check if the join key references the indexed column
        leftKey match {
          case attr: AttributeReference => attr.exprId == indexedAttr.exprId
          case _                        => false
        }
      case _ => false
    }
  }

  /**
   * Checks if a join can use the right side's index.
   *
   * Same logic as [[joiningIndexedColumnLeft]] but for the right side.
   *
   * @param left      The left side of the join
   * @param right     The right side of the join (must be IndexedBlockRDD)
   * @param joinType  The type of join
   * @param condition The join condition
   * @return true if the join can use the right side's index
   */
  def joiningIndexedColumnRight(left: LogicalPlan, right: IndexedBlockRDD, joinType: JoinType, condition: Option[Expression]): Boolean = {
    Join(left, right, joinType, condition, JoinHint.NONE) match {
      // Extract equi-join keys from the join condition
      case ExtractEquiJoinKeys(_, _, rightKeys, _, _, _, rChild, _) =>
        // Only support single-key joins; composite keys are not supported by IndexedJoin
        if (rightKeys.length != 1) return false
        val rightColNo = rChild.output.indexWhere(_.semanticEquals(rightKeys.head))
        rightColNo == right.rdd.colNo
      case _ => false
    }
  }

  /**
   * Checks if a join can use the right side's index when Project is present.
   *
   * Similar to [[joiningIndexedColumnRight]] but handles cases where a Project
   * sits between the IndexedBlockRDD and the Join.
   */
  def joiningIndexedColumnRightThroughProject(
      left: LogicalPlan,
      right: LogicalPlan,
      indexedChild: IndexedBlockRDD,
      joinType: JoinType,
      condition: Option[Expression]
  ): Boolean = {
    Join(left, right, joinType, condition, JoinHint.NONE) match {
      case ExtractEquiJoinKeys(_, _, rightKeys, _, _, _, rChild, _) =>
        if (rightKeys.length != 1) return false
        val rightKey = rightKeys.head
        val indexedColNo = indexedChild.rdd.colNo
        val indexedAttr = indexedChild.output(indexedColNo)
        rightKey match {
          case attr: AttributeReference => attr.exprId == indexedAttr.exprId
          case _                        => false
        }
      case _ => false
    }
  }

  // ===========================================================================
  // Filter Condition Validation
  // ===========================================================================

  /**
   * Checks if a filter condition targets the indexed column.
   *
   * For index-based filtering to apply, the filter must be on the same column
   * that was used to create the index.
   *
   * @param child             The indexed data being filtered
   * @param attributeReference The column being filtered on
   * @return true if the filter is on the indexed column
   */
  def filterIndexedColumn(child: IndexedBlockRDD, attributeReference: AttributeReference): Boolean = {
    val indexedColNo = child.rdd.colNo
    val indexedAttrRef = child.output(indexedColNo)
    attributeReference.exprId == indexedAttrRef.exprId
  }

  /**
   * Extracts AttributeReference from the left or right side of an EqualTo.
   *
   * @param condition The EqualTo condition
   * @return Some(attr) if either side is an AttributeReference, None otherwise
   */
  def extractAttrFromEquality(condition: EqualTo): Option[AttributeReference] = {
    condition match {
      case EqualTo(attr: AttributeReference, _) => Some(attr)
      case EqualTo(_, attr: AttributeReference) => Some(attr)
      case _                                    => None
    }
  }

  /**
   * Checks if join and filter conditions reference the same columns.
   *
   * Used for optimizing patterns like:
   * {{{
   *   SELECT * FROM a JOIN b ON a.id = b.id
   *   WHERE a.id = 123 AND b.id = 123
   * }}}
   *
   * Handles both `attr = lit` and `lit = attr` forms in filter conditions.
   *
   * @param joinCondition  The join equality condition
   * @param leftCondition  The filter condition on the left side
   * @param rightCondition The filter condition on the right side
   * @return true if all conditions reference the same columns
   */
  def joinSameFilterColumns(joinCondition: EqualTo, leftCondition: EqualTo, rightCondition: EqualTo): Boolean = {
    // Extract join column attributes (join condition should be attr = attr)
    val joinLeftAttr = joinCondition.left match {
      case attr: AttributeReference => Some(attr)
      case _                        => None
    }
    val joinRightAttr = joinCondition.right match {
      case attr: AttributeReference => Some(attr)
      case _                        => None
    }

    // Extract filter attributes (handles both attr = lit and lit = attr)
    val leftFilterAttr = extractAttrFromEquality(leftCondition)
    val rightFilterAttr = extractAttrFromEquality(rightCondition)

    // Check if left filter attr matches join left and right filter attr matches join right
    (joinLeftAttr, joinRightAttr, leftFilterAttr, rightFilterAttr) match {
      case (Some(jl), Some(jr), Some(lf), Some(rf)) =>
        (jl.semanticEquals(lf) && jr.semanticEquals(rf)) || (jl.semanticEquals(rf) && jr.semanticEquals(lf))
      case _ => false
    }
  }

  // ===========================================================================
  // Plan Transformation Rules
  // ===========================================================================

  /**
   * Applies transformation rules to convert standard operators to indexed operators.
   *
   * Uses `transformUp` to process the plan bottom-up, ensuring child nodes
   * are transformed before their parents.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

    // -------------------------------------------------------------------------
    // Rule 1: Cache Detection
    // -------------------------------------------------------------------------
    // Converts InMemoryRelation (Spark's cache representation) with an indexed
    // child into IndexedBlockRDD for subsequent indexed operations.

    case InMemoryRelationMatcher(output, storageLevel, child: IndexedOperatorExec) =>
      IndexedBlockRDD(output, getIfCached(child, storageLevel), child)

    // -------------------------------------------------------------------------
    // Rule 2: Indexed Join (Left Side Indexed)
    // -------------------------------------------------------------------------
    // Converts equi-join to IndexedJoin when the left side is indexed.
    // Only INNER JOIN is supported; other join types fall back to Spark's default.

    case Join(left: IndexedBlockRDD, right, joinType @ Inner, condition, _) if joiningIndexedColumnLeft(left, right, joinType, condition) =>
      IndexedJoin(left.asInstanceOf[IndexedOperator], right, joinType, condition)

    // -------------------------------------------------------------------------
    // Rule 3: Indexed Join (Right Side Indexed)
    // -------------------------------------------------------------------------
    // Converts equi-join to IndexedJoin when the right side is indexed.

    case Join(left, right: IndexedBlockRDD, joinType @ Inner, condition, _) if joiningIndexedColumnRight(left, right, joinType, condition) =>
      IndexedJoin(left, right.asInstanceOf[IndexedOperator], joinType, condition)

    // -------------------------------------------------------------------------
    // Rule 2b: Indexed Join through Project (Left Side Indexed)
    // -------------------------------------------------------------------------
    // Handles cases where Catalyst's ColumnPruning has inserted a Project
    // between the IndexedBlockRDD and the Join. We detect this pattern and
    // convert to IndexedJoin, then apply the Project after the join.
    //
    // Before:
    //   Join
    //   ├── Project [subset]
    //   │   └── IndexedBlockRDD [all columns]
    //   └── other
    //
    // After:
    //   Project [subset ++ other.output]
    //   └── IndexedJoin
    //       ├── IndexedBlockRDD [all columns]
    //       └── other

    case Join(ExtractIndexedBlockRDD(indexedChild, Some(project)), right, joinType @ Inner, condition, _)
        if joiningIndexedColumnLeftThroughProject(project, indexedChild, right, joinType, condition) =>
      // Create IndexedJoin using the full IndexedBlockRDD (not the projected subset)
      val indexedJoin = IndexedJoin(indexedChild.asInstanceOf[IndexedOperator], right, joinType, condition)
      // Apply projection to the join output to maintain expected schema
      // The project's expressions reference the IndexedBlockRDD's attributes,
      // which are still valid in the IndexedJoin's output
      val projectExprs = project.projectList ++ right.output
      Project(projectExprs, indexedJoin)

    // -------------------------------------------------------------------------
    // Rule 3b: Indexed Join through Project (Right Side Indexed)
    // -------------------------------------------------------------------------
    // Same as Rule 2b but for the right side.

    case Join(left, ExtractIndexedBlockRDD(indexedChild, Some(project)), joinType @ Inner, condition, _)
        if joiningIndexedColumnRightThroughProject(left, project, indexedChild, joinType, condition) =>
      val indexedJoin = IndexedJoin(left, indexedChild.asInstanceOf[IndexedOperator], joinType, condition)
      val projectExprs = left.output ++ project.projectList
      Project(projectExprs, indexedJoin)

    // -------------------------------------------------------------------------
    // Rule 4: IsNotNull Elimination
    // -------------------------------------------------------------------------
    // Removes redundant IsNotNull checks on indexed data.
    // Since indexed data is already validated during index creation,
    // these null checks are unnecessary.

    case Filter(IsNotNull(attr: AttributeReference), child: IndexedBlockRDD) =>
      child

    case Filter(And(left, IsNotNull(attr: AttributeReference)), child: IndexedBlockRDD) =>
      Filter(left, child)

    case Filter(And(IsNotNull(attr: AttributeReference), right), child: IndexedBlockRDD) =>
      Filter(right, child)

    // -------------------------------------------------------------------------
    // Rule 5: Indexed Filter
    // -------------------------------------------------------------------------
    // Converts equality filter on indexed column to IndexedFilter.
    // This enables O(1) lookup instead of full scan.
    // Handles both `attr = lit` and `lit = attr` forms.

    case Filter(condition @ EqualTo(_: Literal, attr: AttributeReference), child: IndexedBlockRDD) if filterIndexedColumn(child, attr) =>
      IndexedFilter(condition.withNewChildren(condition.children.reverse), child.asInstanceOf[IndexedOperator])

    case Filter(condition @ EqualTo(attr: AttributeReference, _: Literal), child: IndexedBlockRDD) if filterIndexedColumn(child, attr) =>
      IndexedFilter(condition, child.asInstanceOf[IndexedOperator])

    // -------------------------------------------------------------------------
    // Rule 6: Filter-Join Optimization
    // -------------------------------------------------------------------------
    // Optimizes joins where both sides have equality filters on the join column.
    // This pattern occurs with parameterized queries joining on indexed columns.

    case Join(
          left @ IndexedFilter(conditionLeft: EqualTo, _),
          right @ IndexedFilter(conditionRight: EqualTo, rightChild),
          joinType @ Inner,
          Some(condition: EqualTo),
          _
        ) if joinSameFilterColumns(condition, conditionLeft, conditionRight) =>
      IndexedJoin(left, rightChild.asInstanceOf[IndexedOperator], joinType, Some(condition))

    // -------------------------------------------------------------------------
    // Rule 7: Flatten Nested Projects
    //   Rule 2b and 3b may introduce nested Projects.
    //   This rule flattens them to simplify the plan.
    // -------------------------------------------------------------------------

    case Project(projectList, child @ Project(_, child2)) =>
      // Flatten nested Projects
      Project(projectList, child2)
  }
}
