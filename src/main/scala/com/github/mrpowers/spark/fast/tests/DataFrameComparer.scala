package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.SeqLikesExtensions.SeqExtensions
import org.apache.spark.sql.{DataFrame, Row}
trait DataFrameComparer extends DatasetComparer {

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      truncate: Int = 500,
  ): Unit = {
    SchemaComparer.assertSchemaEqual(actualDF, expectedDF, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
    val actual = if (ignoreColumnOrder) orderColumns(actualDF, expectedDF) else actualDF
    if (orderedComparison)
      assertSmallDataFrameEquality(actual, expectedDF, truncate)
    else
      assertSmallDataFrameEquality(defaultSortDataset(actual), defaultSortDataset(expectedDF), truncate)
  }

  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame, truncate: Int): Unit = {
    val a = actualDF.collect()
    val e = expectedDF.collect()
    if (!a.toSeq.approximateSameElements(e, (o1: Row, o2: Row) => o1.equals(o2))) {
      val arr = ("Actual Content", "Expected Content")
      val msg = "Diffs\n" ++ DataframeUtil.showDataframeDiff(arr, a, e, truncate)
      throw DatasetContentMismatch(msg)
    }
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertLargeDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false
  ): Unit = {
    assertLargeDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable = ignoreNullable,
      ignoreColumnNames = ignoreColumnNames,
      orderedComparison = orderedComparison,
      ignoreColumnOrder = ignoreColumnOrder
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertApproximateSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false
  ): Unit = {
    assertSmallDatasetEquality[Row](
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      equals = RowComparer.areRowsEqual(_, _, precision)
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertApproximateLargeDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false
  ): Unit = {
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = RowComparer.areRowsEqual(_, _, precision),
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder
    )
  }
}
