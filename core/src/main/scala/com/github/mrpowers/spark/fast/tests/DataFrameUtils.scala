package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Nondeterministic}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{Column, Dataset}

case class PartitionMonotonicallyIncreasingID() extends LeafExpression with Nondeterministic {

  /**
   * Adapted From org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
   *
   * Record ID within each partition. By being transient, count's value is reset to 0 every time we serialize and deserialize and initialize it.
   */
  @transient private[this] var count: Long = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    count = 1L
  }

  override def stateful: Boolean = true

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def evalInternal(input: InternalRow): Long = {
    val currentCount = count
    count += 1
    currentCount
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val countTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "count")
    ctx.addPartitionInitializationStatement(s"$countTerm = 1L;")

    ev.copy(
      code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $countTerm;
      $countTerm++;""",
      isNull = FalseLiteral
    )
  }
}

object DataframeUtils {
  // from https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex)
  private def zipWithIndex[T](ds: Dataset[T], offset: Long = 0, indexName: String = "index"): Dataset[T] = {
    val id                    = java.util.UUID.randomUUID.toString
    val partitionIdField      = s"partition_$id"
    val incIdField            = s"inc_$id"
    val partitionOffsetsField = s"partition_offset_$id"
    val dfWithPartitionId =
      ds.withColumn(partitionIdField, spark_partition_id()).withColumn(incIdField, new Column(PartitionMonotonicallyIncreasingID()))

    // collect each partition size, create the offset pages
    val partitionOffsets = dfWithPartitionId
      .groupBy(partitionIdField)
      .agg(
        max(incIdField) as partitionOffsetsField
      )
      .select(
        col(partitionIdField),
        sum(partitionOffsetsField).over(Window.orderBy(partitionIdField)) - col(partitionOffsetsField) + lit(offset) as partitionOffsetsField
      )

    // and re-number the index
    dfWithPartitionId
      .join(partitionOffsets, partitionIdField)
      .withColumn(indexName, col(partitionOffsetsField) + col(incIdField))
      .drop(partitionIdField, partitionOffsetsField, incIdField)
      .as(ds.encoder)
  }

  implicit class DatasetUtils[T](df: Dataset[T]) {
    def zipWithIndex(indexName: String): Dataset[T] = DataframeUtils.zipWithIndex(df, 0, indexName)
  }
}
