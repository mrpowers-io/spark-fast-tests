package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DatasetUtils.DatasetOps
import com.github.mrpowers.spark.fast.tests.DatasetComparer.maxUnequalRowsToShow
import com.github.mrpowers.spark.fast.tests.SeqLikesExtensions.SeqExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.reflect.ClassTag

case class DatasetContentMismatch(smth: String) extends Exception(smth)
case class DatasetCountMismatch(smth: String)   extends Exception(smth)

trait DatasetComparer {
  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '$actualCount'
Expected DataFrame Row Count: '$expectedCount'
"""
  }

  private def unequalRDDMessage[T](unequalRDD: RDD[(Long, (T, T))], length: Int): String = {
    "\nRow Index | Actual Row | Expected Row\n" + unequalRDD
      .take(length)
      .map { case (idx, (left, right)) =>
        ufansi.Color.Red(s"$idx | $left | $right")
      }
      .mkString("\n")
  }

  /**
   * order ds1 column according to ds2 column order
   */
  def orderColumns[T](ds1: Dataset[T], ds2: Dataset[T]): Dataset[T] = {
    ds1.select(ds2.columns.map(col).toIndexedSeq: _*).as[T](ds2.encoder)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      equals: (T, T) => Boolean = (o1: T, o2: T) => o1.equals(o2)
  ): Unit = {
    SchemaComparer.assertDatasetSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertSmallDatasetContentEquality(actual, expectedDS, orderedComparison, truncate, equals)
  }

  def assertSmallDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      orderedComparison: Boolean,
      truncate: Int,
      equals: (T, T) => Boolean
  ): Unit = {
    if (orderedComparison)
      assertSmallDatasetContentEquality(actualDS, expectedDS, truncate, equals)
    else
      assertSmallDatasetContentEquality(defaultSortDataset(actualDS), defaultSortDataset(expectedDS), truncate, equals)
  }

  def assertSmallDatasetContentEquality[T: ClassTag](actualDS: Dataset[T], expectedDS: Dataset[T], truncate: Int, equals: (T, T) => Boolean): Unit = {
    val a = actualDS.collect().toSeq
    val e = expectedDS.collect().toSeq
    if (!a.approximateSameElements(e, equals)) {
      val arr = ("Actual Content", "Expected Content")
      val msg = "Diffs\n" ++ ProductUtil.showProductDiff(arr, Left(a -> e), truncate)
      throw DatasetContentMismatch(msg)
    }
  }

  def defaultSortDataset[T](ds: Dataset[T]): Dataset[T] = ds.sort(ds.columns.map(col).toIndexedSeq: _*)

  def sortPreciseColumns[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.dtypes
      .withFilter { dtype =>
        !Seq("DoubleType", "DecimalType", "FloatType").contains(dtype._2)
      }
      .map(_._1)
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertLargeDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean = (o1: T, o2: T) => o1.equals(o2),
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    // first check if the schemas are equal
    SchemaComparer.assertDatasetSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertLargeDatasetContentEquality(actual, expectedDS, equals, orderedComparison)
  }

  def assertLargeDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      orderedComparison: Boolean
  ): Unit = {
    if (orderedComparison) {
      assertLargeDatasetContentEquality(actualDS, expectedDS, equals)
    } else {
      assertLargeDatasetContentEquality(sortPreciseColumns(actualDS), sortPreciseColumns(expectedDS), equals)
    }
  }

  def assertLargeDatasetContentEquality[T: ClassTag](ds1: Dataset[T], ds2: Dataset[T], equals: (T, T) => Boolean): Unit = {
    try {
      val ds1RDD = ds1.rdd.cache()
      val ds2RDD = ds2.rdd.cache()

      val actualCount   = ds1RDD.count
      val expectedCount = ds2RDD.count

      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
      }
      val expectedIndexValue = RddHelpers.zipWithIndex(ds1RDD)
      val resultIndexValue   = RddHelpers.zipWithIndex(ds2RDD)
      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter { case (_, (o1, o2)) =>
          !equals(o1, o2)
        }

      if (!unequalRDD.isEmpty()) {
        throw DatasetContentMismatch(
          unequalRDDMessage(unequalRDD, maxUnequalRowsToShow)
        )
      }

    } finally {
      ds1.rdd.unpersist()
      ds2.rdd.unpersist()
    }
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal. It is recommended to provide `primaryKeys` to ensure accurate and efficient
   * comparison of rows. If primary key is not provided, will try to compare the rows based on their row number. This requires both datasets to be
   * partitioned in the same way and become unreliable when shuffling is involved.
   * @param primaryKeys
   *   unique identifier for each row to ensure accurate comparison of rows
   * @param checkKeyUniqueness
   *   if true, will check if the primary key is actually unique
   */
  def assertLargeDatasetEqualityV2[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: Either[(T, T) => Boolean, Option[Column]] = Right(None),
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      checkKeyUniqueness: Boolean = false,
      primaryKeys: Seq[String] = Seq.empty,
      truncate: Int = 500
  ): Unit = {
    // first check if the schemas are equal
    SchemaComparer.assertDatasetSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertLargeDatasetContentEqualityV2(actual, expectedDS, equals, primaryKeys, checkKeyUniqueness, truncate)
  }

  def assertLargeDatasetContentEqualityV2[T: ClassTag](
      ds1: Dataset[T],
      ds2: Dataset[T],
      equals: Either[(T, T) => Boolean, Option[Column]],
      primaryKeys: Seq[String],
      checkKeyUniqueness: Boolean,
      truncate: Int
  ): Unit = {
    try {
      ds1.cache()
      ds2.cache()

      val actualCount   = ds1.count
      val expectedCount = ds2.count

      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
      }

      if (primaryKeys.nonEmpty && checkKeyUniqueness) {
        assert(ds1.isKeyUnique(primaryKeys), "Primary key is not unique in actual dataset")
        assert(ds2.isKeyUnique(primaryKeys), "Primary key is not unique in expected dataset")
      }

      val joinedDf = ds1
        .joinPair(ds2, primaryKeys)

      val unequalDS = equals match {
        case Left(customEquals) =>
          joinedDf.filter((p: (T, T)) =>
            // dataset joinWith implicitly return null each side for missing values from outer join even for primitive types
            p match {
              case (null, null) => false
              case (null, _)    => true
              case (_, null)    => true
              case (l, r)       => !customEquals(l, r)
            }
          )

        case Right(equalExprOption) =>
          joinedDf
            .filter(equalExprOption.getOrElse(col("actual") =!= col("expected")))
      }

      if (!unequalDS.isEmpty) {
        val joined = Right(unequalDS.take(truncate).toSeq)
        val arr    = ("Actual Content", "Expected Content")
        val msg    = "Diffs\n" ++ ProductUtil.showProductDiff(arr, joined, truncate)
        throw DatasetContentMismatch(msg)
      }

    } finally {
      ds1.unpersist()
      ds2.unpersist()
    }
  }

  def assertApproximateDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    val e = (r1: Row, r2: Row) => {
      r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, precision)
    }
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = e,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata
    )
  }
}

object DatasetComparer {
  val maxUnequalRowsToShow = 10
}
