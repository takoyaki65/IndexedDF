package indexeddataframe

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.storage.StorageLevel

object Utils {
  def defaultNoPartitions: Int = 16
  val defaultPartitioner: HashPartitioner = new HashPartitioner(defaultNoPartitions)

  /** function that is executed when the index is created on a DataFrame this function is called for each partition of the original DataFrame and it
    * creates an [[InternalIndexedPartition]] that contains the rows of the partitions and a CTrie for storing an index
    * @param colNo
    * @param rows
    * @param output
    * @param rddId       RDD ID for Spark memory block identification
    * @param partitionId Partition ID within the RDD
    * @return
    */
  def doIndexing(
      colNo: Int,
      rows: Iterator[InternalRow],
      output: Seq[Attribute],
      rddId: Int,
      partitionId: Int
  ): InternalIndexedPartition = {
    val idf = new InternalIndexedPartition
    idf.initialize(rddId, partitionId)
    idf.createIndex(output, colNo)
    idf.appendRows(rows)
    idf.finishIndexing()
    idf
  }

  /** function that ensures an RDD is cached
    * @param rdd
    * @param storageLevel
    * @tparam T
    * @return
    */
  def ensureCached[T](rdd: IRDD, storageLevel: StorageLevel): IRDD = {
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }

  def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }
}

/** a custom RDD class that is composed of a number of partitions containing the rows of the indexed dataframe; each of these partitions is
  * represented as an [[InternalIndexedPartition]]
  * @param colNo
  * @param partitionsRDD
  */
class IRDD(val colNo: Int, var partitionsRDD: RDD[InternalIndexedPartition])
    extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[InternalIndexedPartition].iterator(part, context).next.iterator
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD = partitionsRDD.persist(newLevel)
    this
  }

  /** Returns the storage level of the underlying partitionsRDD */
  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  /** RDD function that returns an RDD of the rows containing the search key
    * @param key
    * @return
    */
  def get(key: AnyVal): RDD[InternalRow] = {
    // println("I have this many partitions: " + partitionsRDD.getNumPartitions)
    val res = partitionsRDD.flatMap { part =>
      part.get(key).map(row => row.copy())
    }
    res
  }

  /** RDD method that returns an RDD of rows containing the searched keys
    * @param keys
    * @return
    */
  def multiget(keys: Array[AnyVal]): RDD[InternalRow] = {
    // println("I have this many partitions: " + partitionsRDD.getNumPartitions)
    val res = partitionsRDD.mapPartitions[InternalRow](part => part.next().multiget(keys), true)
    res

  }

  /**
    * Returns the total size in bytes of all partitions in this IRDD.
    * This is used for Catalyst statistics to enable proper join strategy selection.
    *
    * Note: This triggers an RDD action if the RDD is not cached. For cached RDDs,
    * the computation is efficient as InternalIndexedPartition tracks totalMemoryUsed.
    */
  def sizeInBytes: Long = {
    partitionsRDD.map(_.totalMemoryUsed).reduce(_ + _)
  }

  /** multiget method used in the broadcast join
    * @param rightRDD
    * @param leftSchema
    * @param rightSchema
    * @param joinRightCol
    * @return
    */
  def multigetBroadcast(
      rightRDD: Broadcast[Array[InternalRow]],
      leftSchema: StructType,
      rightSchema: StructType,
      rightOutput: Seq[Attribute],
      joinRightCol: Int
  ): RDD[InternalRow] = {
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => {
        val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
        val res = part
          .next()
          .multigetJoinedRight(
            rightRDD.value.toIterator,
            joiner,
            rightOutput,
            joinRightCol
          )
        res
      },
      true
    )
    res
  }
}
