package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.snowflake.snowpark.DataFrame

/**
 * Provides assertion utilities for Snowpark DataFrames.
 *
 * Usage:
 * {{{
 * import com.github.mrpowers.spark.fast.tests.SnowparkDataFrameComparer
 *
 * class MyTest extends AnyFunSuite with SnowparkDataFrameComparer {
 *   test("dataframes match") {
 *     assertSmallDataFrameEquality(actualDF, expectedDF)
 *   }
 * }
 * }}}
 */
trait SnowparkDataFrameComparer extends DataFrameLikeComparer {

  implicit val snowparkDataFrameLike: DataFrameLike[DataFrame, RowLike] = SnowparkDataFrameLike.instance

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal. This is for small DataFrames that can be collected to the driver.
   */
  def assertSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): Unit = {
    assertDataFrameLikeEquality[DataFrame, RowLike](
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      truncate,
      outputFormat = outputFormat
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are approximately equal. Useful for comparing DataFrames with floating-point values. This is
   * for small DataFrames that can be collected to the driver.
   */
  def assertApproximateSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): Unit = {
    assertApproximateDataFrameLikeEquality[DataFrame](
      actualDF,
      expectedDF,
      precision,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      truncate,
      outputFormat
    )
  }

  /**
   * Raises an error unless the schemas of `actualDF` and `expectedDF` are equal.
   */
  def assertSchemaEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    SchemaLikeComparer.assertSchemaEqual(
      snowparkDataFrameLike.schema(actualDF),
      snowparkDataFrameLike.schema(expectedDF),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata
    )
  }
}

object SnowparkDataFrameComparer extends SnowparkDataFrameComparer
