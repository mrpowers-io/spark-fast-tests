package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

/**
 * Schema comparison utilities for Spark.
 */
object SchemaComparer {

  /**
   * Asserts that two Spark StructType schemas are equal.
   */
  def assertSchemaEqual(
      actualSchema: StructType,
      expectedSchema: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true,
      outputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  ): Unit = {
    SchemaLikeComparer.assertSchemaEqual(
      SparkSchemaAdapter(actualSchema),
      SparkSchemaAdapter(expectedSchema),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      outputFormat
    )
  }
}
