package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.DatasetUtils.DatasetOps
import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.reflect.ClassTag

/**
 * Provides assertion utilities for Spark Datasets and DataFrames.
 */
trait DatasetComparer extends DataFrameLikeComparer {

  private val primaryKeysRequiredMessage =
    "assertLargeDatasetEquality requires non-empty primaryKeys. Provide stable unique key columns for each row."
  private val defaultEqualityExpr = col("actual") <=> col("expected")

  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '$actualCount'
Expected DataFrame Row Count: '$expectedCount'
"""
  }

  private def requirePrimaryKeys(primaryKeys: Seq[String]): Unit = {
    require(primaryKeys.nonEmpty, primaryKeysRequiredMessage)
  }

  private def mismatchFilter(equalExpr: Column): Column = {
    not(coalesce(equalExpr, lit(false)))
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
   * Raises an error unless `actualDS` and `expectedDS` are equal. Large comparisons are keyed and require non-empty `primaryKeys`.
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: Column = defaultEqualityExpr,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      primaryKeys: Seq[String],
      truncate: Int = 500
  ): Unit = {
    assertLargeDatasetEqualityInternal(
      actualDS,
      expectedDS,
      Right(equals),
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys,
      truncate
    )
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. Large comparisons are keyed and require non-empty `primaryKeys`.
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      primaryKeys: Seq[String]
  ): Unit = {
    assertLargeDatasetEqualityInternal(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable = false,
      ignoreColumnNames = false,
      orderedComparison = true,
      ignoreColumnOrder = false,
      ignoreMetadata = true,
      primaryKeys,
      truncate = 500
    )
  }

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
    assertLargeDatasetEqualityInternal(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison = true,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys,
      truncate = 500
    )
  }

  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      orderedComparison: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      primaryKeys: Seq[String]
  ): Unit = {
    assertLargeDatasetEqualityInternal(
      actualDS,
      expectedDS,
      Left(equals),
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys,
      truncate = 500
    )
  }

  def assertLargeDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      orderedComparison: Boolean,
      primaryKeys: Seq[String],
      truncate: Int = 500
  ): Unit = {
    implicit val datasetLike: DataFrameLike[Dataset[T], T] = SparkDatasetLike.instance[T](expectedDS.encoder)
    assertLargeDatasetContentEqualityInternal(actualDS, expectedDS, Left(equals), orderedComparison, primaryKeys, truncate)
  }

  def assertLargeDatasetContentEquality[T: ClassTag](
      ds1: Dataset[T],
      ds2: Dataset[T],
      equals: (T, T) => Boolean,
      primaryKeys: Seq[String],
      truncate: Int
  ): Unit = {
    implicit val datasetLike: DataFrameLike[Dataset[T], T] = SparkDatasetLike.instance[T](ds2.encoder)
    assertLargeDatasetContentEqualityInternal(ds1, ds2, Left(equals), orderedComparison = true, primaryKeys, truncate)
  }

  private def assertLargeDatasetEqualityInternal[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: Either[(T, T) => Boolean, Column],
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      orderedComparison: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      primaryKeys: Seq[String],
      truncate: Int
  ): Unit = {
    requirePrimaryKeys(primaryKeys)
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
    assertLargeDatasetContentEqualityInternal(actual, expectedDS, equals, orderedComparison, primaryKeys, truncate)
  }

  private def assertLargeDatasetContentEqualityInternal[T: ClassTag](
      ds1: Dataset[T],
      ds2: Dataset[T],
      equals: Either[(T, T) => Boolean, Column],
      orderedComparison: Boolean,
      primaryKeys: Seq[String],
      truncate: Int
  )(implicit datasetLike: DataFrameLike[Dataset[T], T]): Unit = {
    requirePrimaryKeys(primaryKeys)
    try {
      ds1.cache()
      ds2.cache()

      val actualCount   = ds1.count()
      val expectedCount = ds2.count()

      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
      }

      val joinedDS = ds1.joinPair(ds2, primaryKeys)

      val unequalDS = equals match {
        case Left(customEquals) =>
          joinedDS.filter((pair: (T, T)) =>
            pair match {
              case (null, null) => false
              case (null, _)    => true
              case (_, null)    => true
              case (left, right) =>
                !customEquals(left, right)
            }
          )
        case Right(equalExpr) =>
          joinedDS.filter(mismatchFilter(equalExpr))
      }

      val unequalRows = unequalDS.take(truncate).toSeq
      if (unequalRows.nonEmpty) {
        val msg = "Diffs\n" ++ ProductLikeUtil.showProductDiffWithHeader(
          Seq("Actual Content", "Expected Content"),
          datasetLike.columns(ds1),
          unequalRows.map(_._1),
          unequalRows.map(_._2),
          truncate
        )
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
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      primaryKeys: Seq[String]
  ): Unit = {
    val equals = (r1: Row, r2: Row) => {
      r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, precision)
    }
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = equals,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      primaryKeys
    )
  }
}

object DatasetComparer {
  val maxUnequalRowsToShow = 10
}
