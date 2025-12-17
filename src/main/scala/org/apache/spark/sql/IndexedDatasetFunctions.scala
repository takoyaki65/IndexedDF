package org.apache.spark.sql

import indexeddataframe.logical.{CreateIndex, GetRows}
import org.apache.spark.sql.classic.{ClassicConversions, Dataset => ClassicDataset}

/** we add indexed methods to the dataset class with these, users can create indexes and get rows based on key also indexed equi joins are supported
  * if the left side of the join is an indexed relation
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
  def getRows(key: AnyVal): DataFrame = {
    ClassicDataset.ofRows(classicDs.sparkSession, GetRows(key, classicDs.logicalPlan))
  }
}
