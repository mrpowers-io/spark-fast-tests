package com.github.mrpowers.spark.fast.tests

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
      truncate: Int = 500
  ): Unit = {
    assertSmallDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      truncate
    )
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

  def assertApproximateSmallDataFrameEquality(actualDF: DataFrame,
                                              expectedDF: DataFrame,
                                              precision: Double,
                                              ignoreNullable: Boolean = false,
                                              ignoreColumnNames: Boolean = false,
                                              orderedComparison: Boolean = true,
                                              ignoreColumnOrder: Boolean = false): Unit = {
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

  def assertApproximateLargeDataFrameEquality(actualDF: DataFrame,
                                         expectedDF: DataFrame,
                                         precision: Double,
                                         ignoreNullable: Boolean = false,
                                         ignoreColumnNames: Boolean = false,
                                         orderedComparison: Boolean = true,
                                         ignoreColumnOrder: Boolean = false): Unit = {
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
