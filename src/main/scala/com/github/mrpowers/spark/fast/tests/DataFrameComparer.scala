package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.DataFrame

trait DataFrameComparer extends DatasetComparer {

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertSmallDataFrameEquality(
    actualDF: DataFrame,
    expectedDF: DataFrame,
    ignoreNullable: Boolean = false,
    orderedComparison: Boolean = true
  ): Unit = {
    assertSmallDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      orderedComparison
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertLargeDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assertLargeDatasetEquality(
      actualDF,
      expectedDF
    )
  }

}
