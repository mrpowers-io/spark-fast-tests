package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import scala.reflect.ClassTag

private object DatasetUtils {
  implicit class DatasetOps[T: ClassTag](ds: Dataset[T]) {
    def joinPair[P: ClassTag](
        other: Dataset[P],
        primaryKeys: Seq[String]
    ): Dataset[(T, P)] = {
      ds
        .as("l")
        .joinWith(other.as("r"), primaryKeys.map(k => col(s"l.$k") <=> col(s"r.$k")).reduce(_ && _), "full_outer")
        .select(
          col("_1").as[T](ds.encoder).name("actual"),
          col("_2").as[P](other.encoder).name("expected")
        )
    }
  }
}
