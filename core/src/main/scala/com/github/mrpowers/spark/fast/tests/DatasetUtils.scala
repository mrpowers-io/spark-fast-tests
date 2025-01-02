package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, TypedColumn}

import scala.reflect.ClassTag

private object DatasetUtils {
  implicit class DatasetOps[T: ClassTag](ds: Dataset[T]) {
    def zipWithIndex(indexName: String): DataFrame = ds
      .orderBy()
      .withColumn(indexName, row_number().over(Window.orderBy(monotonically_increasing_id())))
      .select(ds.columns.map(col) :+ col(indexName): _*)

    /**
     * Check if the primary key is actually unique
     */
    def isKeyUnique(primaryKey: Seq[String]): Boolean =
      ds.select(primaryKey.map(col): _*).distinct.count == ds.count

    def joinPair[P: ClassTag](
        other: Dataset[P],
        primaryKeys: Seq[String]
    ): Dataset[(T, P)] = {
      if (primaryKeys.nonEmpty) {
        ds
          .as("l")
          .joinWith(other.as("r"), primaryKeys.map(k => col(s"l.$k") === col(s"r.$k")).reduce(_ && _), "full_outer")
      } else {
        val indexName = s"index_${java.util.UUID.randomUUID}"
        val joined = ds
          .zipWithIndex(indexName)
          .alias("l")
          .joinWith(other.zipWithIndex(indexName).alias("r"), col(s"l.$indexName") === col(s"r.$indexName"), "full_outer")

        joined
          .select(
            encoderToCol("_1", ds.schema, ds.encoder, Seq(indexName)),
            encoderToCol("_2", other.schema, other.encoder, Seq(indexName))
          )
      }
    }
  }

  private def encoderToCol[P: ClassTag](colName: String, schema: StructType, encoder: Encoder[P], key: Seq[String]): TypedColumn[Any, P] = {
    val columns   = schema.names.map(n => col(s"$colName.$n")) // name from encoder is not reliable
    val isRowType = implicitly[ClassTag[P]].runtimeClass == classOf[Row]
    val unTypedColumn =
      if (columns.length == 1 && !isRowType)
        columns.head
      else
        when(key.map(k => col(s"$colName.$k").isNull).reduce(_ && _), lit(null)).otherwise(struct(columns: _*))

    unTypedColumn.as(colName).as[P](encoder)
  }
}
