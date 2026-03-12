package com.github.mrpowers.spark.fast.tests.api

/**
 * Common exception types for DataFrame comparison. These are framework-agnostic and used by both Spark and Snowpark.
 */
case class DatasetContentMismatch(smth: String) extends Exception(smth)
case class DatasetSchemaMismatch(smth: String)  extends Exception(smth)
case class DatasetCountMismatch(smth: String)   extends Exception(smth)
case class ColumnMismatch(smth: String)         extends Exception(smth)
case class ColumnOrderMismatch(smth: String)    extends Exception(smth)
