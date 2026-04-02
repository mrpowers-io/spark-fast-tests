package com.github.mrpowers.spark.fast.tests.api

/**
 * Type class for DataFrame-like operations that work across different DataFrame implementations (Spark, Snowpark, etc.)
 *
 * @tparam F
 *   the DataFrame type
 * @tparam R
 *   the row type
 */
trait DataFrameLike[F, +R] {

  /** Returns the schema of the DataFrame */
  def schema(df: F): SchemaLike

  /** Collects all rows from the DataFrame */
  def collect(df: F): Seq[R]

  /** Returns the column names */
  def columns(df: F): Array[String]

  /** Returns the number of rows */
  def count(df: F): Long

  /** Selects columns by name and returns a new DataFrame */
  def select(df: F, columns: Seq[String]): F

  /** Sorts the DataFrame by all columns */
  def sort(df: F): F

  /** Returns the data types of columns as (columnName, typeName) pairs */
  def dtypes(df: F): Array[(String, String)]
}
