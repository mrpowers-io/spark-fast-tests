package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DatasetUtils.DatasetOps
import com.github.mrpowers.spark.fast.tests.DatasetComparer.maxUnequalRowsToShow
import com.github.mrpowers.spark.fast.tests.SeqLikesExtensions.SeqExtensions
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.reflect.ClassTag

trait DatasetComparerV2 {
  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '$actualCount'
Expected DataFrame Row Count: '$expectedCount'
"""
  }

  /**
   * order ds1 column according to ds2 column order
   */
  def orderColumns[T](ds1: Dataset[T], ds2: Dataset[T]): Dataset[T] = {
    ds1.select(ds2.columns.map(col).toIndexedSeq: _*).as[T](ds2.encoder)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   * @param primaryKeys
   *   unique identifier for each row to ensure accurate comparison of rows. Make sure the primaryKeys are unique else the comparison will not be
   *   accurate.
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: Column = col("actual") =!= col("expected"),
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      primaryKeys: Seq[String] = Seq.empty,
      truncate: Int = 500
  ): Unit = {
    assertLargeDatasetEquality(
      actualDS,
      expectedDS,
      Right(Some(equals)),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys,
      truncate
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean
  ): Unit = {
    assertLargeDatasetEquality(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable = false,
      ignoreColumnNames = false,
      ignoreColumnOrder = false,
      ignoreMetadata = true,
      Seq.empty,
      500
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   * @param primaryKeys
   *   unique identifier for each row to ensure accurate comparison of rows
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      primaryKeys: Seq[String]
  ): Unit = {
    assertLargeDatasetEquality(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable = false,
      ignoreColumnNames = false,
      ignoreColumnOrder = false,
      ignoreMetadata = true,
      primaryKeys,
      500
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   * @param primaryKeys
   *   unique identifier for each row to ensure accurate comparison of rows
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      primaryKeys: Seq[String]
  ): Unit = {
    assertLargeDatasetEquality(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys,
      500
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   * @param primaryKeys
   *   unique identifier for each row to ensure accurate comparison of rows
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: Either[(T, T) => Boolean, Option[Column]],
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      primaryKeys: Seq[String],
      truncate: Int
  ): Unit = {
    // first check if the schemas are equal
    SchemaComparer.assertDatasetSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertLargeDatasetContentEquality(actual, expectedDS, equals, primaryKeys, truncate)
  }

  private def assertLargeDatasetContentEquality[T: ClassTag](
      ds1: Dataset[T],
      ds2: Dataset[T],
      equals: Either[(T, T) => Boolean, Option[Column]],
      primaryKeys: Seq[String],
      truncate: Int
  ): Unit = {
    try {
      ds1.cache()
      ds2.cache()

      val actualCount   = ds1.count
      val expectedCount = ds2.count

      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
      }

      val joinedDf = ds1
        .joinPair(ds2, primaryKeys)

      val unequalDS = equals match {
        case Left(customEquals) =>
          joinedDf
            .filter((p: (T, T)) =>
              // dataset joinWith implicitly return null each side for missing values from outer join even for primitive types
              p match {
                case (null, null) => false
                case (null, _)    => true
                case (_, null)    => true
                case (l, r)       => !customEquals(l, r)
              }
            )

        case Right(equalExprOption) =>
          joinedDf
            .filter(equalExprOption.getOrElse(col("actual") =!= col("expected")))
      }

      if (!unequalDS.isEmpty) {
        val joined = Right(unequalDS.take(truncate).toSeq)
        val arr    = ("Actual Content", "Expected Content")
        val msg    = "Diffs\n" ++ ProductUtil.showProductDiff(arr, joined, truncate)
        throw DatasetContentMismatch(msg)
      }

    } finally {
      ds1.unpersist()
      ds2.unpersist()
    }
  }

  def assertApproximateDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      primaryKeys: Seq[String] = Seq.empty
  ): Unit = {
    val e = (r1: Row, r2: Row) => {
      r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, precision)
    }
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = e,
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys
    )
  }
}
