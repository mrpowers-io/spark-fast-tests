package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.OptionEncoder
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, TypedColumn}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

private object DatasetUtils {
  implicit class DatasetOps[T: ClassTag: TypeTag](ds: Dataset[T]) {
    def zipWithIndex(indexName: String): DataFrame = ds
      .orderBy()
      .withColumn(indexName, row_number().over(Window.orderBy(monotonically_increasing_id())))
      .select(ds.columns.map(col) :+ col(indexName): _*)

    /**
     * Check if the primary key is actually unique
     */
    def isKeyUnique(primaryKey: Seq[String]): Boolean =
      ds.select(primaryKey.map(col): _*).distinct.count == ds.count

    def outerJoinWith[P: ClassTag: TypeTag](
        other: Dataset[P],
        primaryKeys: Seq[String],
        outerJoinType: String = "full"
    ): Dataset[(Option[T], Option[P])] = {
      val (ds1, ds2, key) = if (primaryKeys.nonEmpty) {
        (ds, other, primaryKeys)
      } else {
        val indexName = s"index_${java.util.UUID.randomUUID}"
        (ds.zipWithIndex(indexName), other.zipWithIndex(indexName), Seq(indexName))
      }

      val joined = ds1
        .as("l")
        .join(ds2.as("r"), key, s"${outerJoinType}_outer")

      joined.select(colOptionTypedCol[T]("l", ds.schema, key), colOptionTypedCol[P]("r", other.schema, key))
    }
  }

  private def colOptionTypedCol[P: ClassTag: TypeTag](
      colName: String,
      schema: StructType,
      key: Seq[String]
  ): TypedColumn[Any, Option[P]] = {
    val columns   = schema.names.map(n => col(s"$colName.$n"))
    val isRowType = implicitly[ClassTag[P]].runtimeClass == classOf[Row]
    val unTypedColumn =
      if (columns.length == 1 && !isRowType)
        columns.head
      else
        when(key.map(k => col(s"$colName.$k").isNull).reduce(_ && _), lit(null)).otherwise(struct(columns: _*))

    val enc: Encoder[Option[P]] = if (isRowType) {
      ExpressionEncoder(OptionEncoder(RowEncoder.encoderFor(schema).asInstanceOf[AgnosticEncoder[P]]))
    } else {
      ExpressionEncoder()
    }
    unTypedColumn.as(colName).as[Option[P]](enc)
  }

  def colToRowCol(
      colName: String,
      schema: StructType
  ): TypedColumn[Any, Row] = {
    val columns = schema.names.map(n => col(s"$colName.$n"))
    struct(columns: _*).as(colName).as[Row](ExpressionEncoder()) // Encoders.row(schema)
  }
}
