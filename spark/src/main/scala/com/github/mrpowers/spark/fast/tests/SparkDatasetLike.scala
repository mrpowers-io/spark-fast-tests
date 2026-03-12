package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.apache.spark.sql.functions.col

/**
 * DataFrameLike instance for Spark Dataset[T]. Provides typed comparison support where T is the element type.
 */
object SparkDatasetLike {

  /**
   * Creates a DataFrameLike instance for Dataset[T].
   */
  def instance[T](implicit enc: Encoder[T]): DataFrameLike[Dataset[T], T] = new DataFrameLike[Dataset[T], T] {

    override def schema(ds: Dataset[T]): SchemaLike = SparkSchemaAdapter(ds.schema)

    override def collect(ds: Dataset[T]): Seq[T] = ds.collect()

    override def columns(ds: Dataset[T]): Array[String] = ds.columns

    override def count(ds: Dataset[T]): Long = ds.count()

    override def select(ds: Dataset[T], columns: Seq[String]): Dataset[T] =
      ds.select(columns.map(col): _*).as[T]

    override def sort(ds: Dataset[T]): Dataset[T] =
      ds.sort(ds.columns.map(col): _*)

    override def dtypes(ds: Dataset[T]): Array[(String, String)] = ds.dtypes
  }
}

/**
 * DataFrameLike instance for Spark DataFrame with RowLike row type. Used for approximate comparisons that need RowLikeComparer.
 */
object SparkDataFrameLike extends DataFrameLike[DataFrame, RowLike] {

  override def schema(df: DataFrame): SchemaLike = SparkSchemaAdapter(df.schema)

  override def collect(df: DataFrame): Seq[RowLike] = {
    df.map(SparkRowAdapter.apply)(org.apache.spark.sql.Encoders.kryo[SparkRowAdapter]).collect()
  }

  override def columns(df: DataFrame): Array[String] = df.columns

  override def count(df: DataFrame): Long = df.count()

  override def select(df: DataFrame, columns: Seq[String]): DataFrame =
    df.select(columns.map(col): _*)

  override def sort(df: DataFrame): DataFrame =
    df.sort(df.columns.map(col): _*)

  override def dtypes(df: DataFrame): Array[(String, String)] = df.dtypes

  implicit val instance: DataFrameLike[DataFrame, RowLike] = this
}
