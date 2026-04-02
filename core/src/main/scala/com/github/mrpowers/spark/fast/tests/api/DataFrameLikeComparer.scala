package com.github.mrpowers.spark.fast.tests.api

import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.api.SeqLikeExtensions.SeqExtensions

import scala.reflect.ClassTag

/**
 * Provides assertion utilities for DataFrame-like types via the DataFrameLike type class.
 */
trait DataFrameLikeComparer {

  /**
   * Orders columns of df1 according to df2's column order.
   */
  def orderColumnsGeneric[F, R](df1: F, df2: F)(implicit ev: DataFrameLike[F, R]): F = {
    ev.select(df1, ev.columns(df2).toSeq)
  }

  /**
   * Sorts the DataFrame by all columns.
   */
  def defaultSortGeneric[F, R](df: F)(implicit ev: DataFrameLike[F, R]): F = ev.sort(df)

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal.
   */
  def assertDataFrameLikeEquality[F, R: ClassTag](
      actualDF: F,
      expectedDF: F,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      equals: (R, R) => Boolean = (o1: R, o2: R) => o1.equals(o2),
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide,
      schemaDiffOutputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  )(implicit ev: DataFrameLike[F, R]): Unit = {
    SchemaLikeComparer.assertSchemaEqual(
      ev.schema(actualDF),
      ev.schema(expectedDF),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      schemaDiffOutputFormat
    )
    val actual = if (ignoreColumnOrder) orderColumnsGeneric(actualDF, expectedDF) else actualDF
    assertDataFrameLikeContentEquality(actual, expectedDF, orderedComparison, truncate, equals, outputFormat)
  }

  /**
   * Compares content of two DataFrames. Generic version that works with any DataFrame type.
   */
  def assertDataFrameLikeContentEquality[F, R: ClassTag](
      actualDF: F,
      expectedDF: F,
      orderedComparison: Boolean,
      truncate: Int,
      equals: (R, R) => Boolean,
      outputFormat: DataframeDiffOutputFormat
  )(implicit ev: DataFrameLike[F, R]): Unit = {
    val (actual, expected) = if (orderedComparison) {
      (actualDF, expectedDF)
    } else {
      (defaultSortGeneric(actualDF), defaultSortGeneric(expectedDF))
    }

    val actualRows   = ev.collect(actual)
    val expectedRows = ev.collect(expected)

    if (!actualRows.approximateSameElements(expectedRows, equals)) {
      val msg = "Diffs\n" ++ ProductLikeUtil.showProductDiff(
        ev.columns(actual),
        actualRows,
        expectedRows,
        truncate,
        outputFormat = outputFormat
      )
      throw DatasetContentMismatch(msg)
    }
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are approximately equal. Generic version that works with any DataFrame type. Uses tolerance
   * for numeric comparisons.
   */
  def assertApproximateDataFrameLikeEquality[F](
      actualDF: F,
      expectedDF: F,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  )(implicit ev: DataFrameLike[F, RowLike]): Unit = {
    assertDataFrameLikeEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      truncate,
      (r1: RowLike, r2: RowLike) => RowLikeComparer.areRowsEqual(r1, r2, precision),
      outputFormat
    )
  }
}

object DataFrameLikeComparer extends DataFrameLikeComparer
