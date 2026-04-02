package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.DatasetComparer.maxUnequalRowsToShow
import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import scala.reflect.ClassTag

/**
 * Provides assertion utilities for Spark Datasets and DataFrames.
 */
trait DatasetComparer extends DataFrameLikeComparer {

  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '$actualCount'
Expected DataFrame Row Count: '$expectedCount'
"""
  }

  private def unequalRDDMessage[T](unequalRDD: RDD[(Long, (T, T))], length: Int): String = {
    "\nRow Index | Actual Row | Expected Row\n" + unequalRDD
      .take(length)
      .map { case (idx, (left, right)) =>
        ufansi.Color.Red(s"$idx | $left | $right")
      }
      .mkString("\n")
  }

  /**
   * Sorts the dataset by columns that are not floating point types (Double, Decimal, Float). Used for unordered comparisons with large datasets to
   * avoid floating point sorting issues.
   */
  def sortPreciseColumns[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.dtypes
      .withFilter { dtype =>
        !Seq("DoubleType", "DecimalType", "FloatType").contains(dtype._2)
      }
      .map(_._1)
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal.
   */
  def assertSmallDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      equals: (T, T) => Boolean = (o1: T, o2: T) => o1.equals(o2),
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): Unit = {
    implicit val datasetLike: DataFrameLike[Dataset[T], T] = SparkDatasetLike.instance[T](expectedDS.encoder)
    assertDataFrameLikeEquality(
      actualDS,
      expectedDS,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      truncate,
      equals,
      outputFormat
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. Optimized for large datasets.
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean = (o1: T, o2: T) => o1.equals(o2),
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    implicit val datasetLike: DataFrameLike[Dataset[T], T] = SparkDatasetLike.instance[T](expectedDS.encoder)
    SchemaLikeComparer.assertSchemaEqual(
      datasetLike.schema(actualDS),
      datasetLike.schema(expectedDS),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata
    )

    val actual = if (ignoreColumnOrder) orderColumnsGeneric(actualDS, expectedDS) else actualDS
    assertLargeDatasetContentEquality(actual, expectedDS, equals, orderedComparison)
  }

  def assertLargeDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      orderedComparison: Boolean
  ): Unit = {
    if (orderedComparison) {
      assertLargeDatasetContentEquality(actualDS, expectedDS, equals)
    } else {
      assertLargeDatasetContentEquality(sortPreciseColumns(actualDS), sortPreciseColumns(expectedDS), equals)
    }
  }

  def assertLargeDatasetContentEquality[T: ClassTag](ds1: Dataset[T], ds2: Dataset[T], equals: (T, T) => Boolean): Unit = {
    try {
      val ds1RDD = ds1.rdd.cache()
      val ds2RDD = ds2.rdd.cache()

      val actualCount   = ds1RDD.count
      val expectedCount = ds2RDD.count

      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
      }
      val expectedIndexValue = RddHelpers.zipWithIndex(ds1RDD)
      val resultIndexValue   = RddHelpers.zipWithIndex(ds2RDD)
      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter { case (_, (o1, o2)) =>
          !equals(o1, o2)
        }

      if (!unequalRDD.isEmpty()) {
        throw DatasetContentMismatch(
          unequalRDDMessage(unequalRDD, maxUnequalRowsToShow)
        )
      }

    } finally {
      ds1.rdd.unpersist()
      ds2.rdd.unpersist()
    }
  }

  def assertApproximateDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
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
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata
    )
  }
}

object DatasetComparer {
  val maxUnequalRowsToShow = 10
}
