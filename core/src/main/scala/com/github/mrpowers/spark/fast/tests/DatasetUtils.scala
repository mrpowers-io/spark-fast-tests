package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.ClassTag

private object DatasetUtils {
  implicit class DatasetOps[T: ClassTag](ds: Dataset[T]) {
    def zipWithIndex(indexName: String): DataFrame = ds
      .orderBy()
      .withColumn(indexName, row_number().over(Window.orderBy(monotonically_increasing_id())))
      .select(ds.columns.map(col) :+ col(indexName): _*)

    def joinPair(
        other: Dataset[T],
        primaryKeys: Seq[String]
    ): Dataset[(T, T)] = {
      if (primaryKeys.nonEmpty) {
        ds
          .as("l")
          .joinWith(other.as("r"), primaryKeys.map(k => col(s"l.$k") === col(s"r.$k")).reduce(_ && _))
      } else {
        val indexName = s"index_${java.util.UUID.randomUUID}"
        val columns   = ds.columns
        val joined = ds
          .zipWithIndex(indexName)
          .alias("l")
          .join(other.zipWithIndex(indexName).alias("r"), indexName)

        val encoder: Encoder[T] = ds.encoder
        val leftCols            = columns.map(n => col(s"l.$n"))
        val rightCols           = columns.map(n => col(s"r.$n"))
        val (pair1, pair2) =
          if (columns.length == 1)
            (leftCols.head, rightCols.head)
          else
            (struct(leftCols: _*), struct(rightCols: _*))

        joined
          .select(
            pair1.as("l").as[T](encoder),
            pair2.as("r").as[T](encoder)
          )
      }
    }
  }
}
