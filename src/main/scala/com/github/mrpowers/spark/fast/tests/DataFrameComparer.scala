package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparerLike.naiveEquality
import org.apache.spark.sql.DataFrame

trait DataFrameComparer extends DatasetComparer {

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertSmallDataFrameEquality(actualDF: DataFrame,
                                   expectedDF: DataFrame,
                                   ignoreNullable: Boolean = false,
                                   ignoreColumnNames: Boolean = false,
                                   orderedComparison: Boolean = true): Unit = {
    assertSmallDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertLargeDataFrameEquality(actualDF: DataFrame,
                                   expectedDF: DataFrame,
                                   ignoreNullable: Boolean = false,
                                   ignoreColumnNames: Boolean = false,
                                   orderedComparison: Boolean = true): Unit = {
    assertLargeDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable = ignoreNullable,
      ignoreColumnNames = ignoreColumnNames,
      orderedComparison = orderedComparison
    )
  }

}
