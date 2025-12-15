package org.apache.spark.sql

import indexeddataframe.logical.{AppendRows, CreateIndex, GetRows}
import org.apache.spark.sql.classic.{ClassicConversions, Dataset => ClassicDataset}

/** we add 3 new "indexed" methods to the dataset class with these, users can create indexes, append rows and get rows based on key also indexed equi
  * joins are supported if the left side of the join is an indexed relation
  * @param ds
  * @tparam T
  */
class IndexedDatasetFunctions[T](ds: Dataset[T]) extends Serializable with ClassicConversions {
  private val classicDs: ClassicDataset[T] = ds.asInstanceOf[ClassicDataset[T]]

  def createIndex(colNo: Int): DataFrame = {
    ClassicDataset.ofRows(classicDs.sparkSession, CreateIndex(colNo, classicDs.logicalPlan))
  }
  def createIndex(colName: String): DataFrame = {
    val colNo = classicDs.schema.fieldIndex(colName)
    createIndex(colNo)
  }
  def appendRows(rightDS: Dataset[T]): DataFrame = {
    val rightClassicDs = rightDS.asInstanceOf[ClassicDataset[T]]
    ClassicDataset.ofRows(classicDs.sparkSession, AppendRows(classicDs.logicalPlan, rightClassicDs.logicalPlan))
  }
  def getRows(key: AnyVal): DataFrame = {
    ClassicDataset.ofRows(classicDs.sparkSession, GetRows(key, classicDs.logicalPlan))
  }
}
