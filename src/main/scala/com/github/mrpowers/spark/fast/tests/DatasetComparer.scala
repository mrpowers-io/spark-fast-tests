package com.github.mrpowers.spark.fast.tests

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

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

  private def betterContentMismatchMessage[T](a: Array[T], e: Array[T], truncate: Int): String = {
    // Diffs\n is a hack, but a newline isn't added in ScalaTest unless we add "Diffs"
    val arr = Array(("Actual Content", "Expected Content")) ++ a.zipAll(e, "": Any, "": Any)
    "Diffs\n" ++ ArrayUtil.showTwoColumnString(arr, truncate)
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
   *  order ds1 column according to ds2 column order
   *  */
  def orderColumns[T](ds1: Dataset[T], ds2: Dataset[T]): Dataset[T] = {
    ds1.select(ds2.columns.map(col).toIndexedSeq: _*).as[T](ds2.encoder)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      truncate: Int = 500
  ): Unit = {
    SchemaComparer.assertSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertSmallDatasetContentEquality(actual, expectedDS, orderedComparison, truncate)
  }

  def assertSmallDatasetContentEquality[T](actualDS: Dataset[T], expectedDS: Dataset[T], orderedComparison: Boolean, truncate: Int): Unit = {
    if (orderedComparison)
      assertSmallDatasetContentEquality(actualDS, expectedDS, truncate)
    else
      assertSmallDatasetContentEquality(defaultSortDataset(actualDS), defaultSortDataset(expectedDS), truncate)
  }

  def assertSmallDatasetContentEquality[T](actualDS: Dataset[T], expectedDS: Dataset[T], truncate: Int): Unit = {
    val a = actualDS.collect()
    val e = expectedDS.collect()
    if (!a.sameElements(e)) {
      throw DatasetContentMismatch(betterContentMismatchMessage(a, e, truncate))
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
      ignoreColumnOrder: Boolean = false
  ): Unit = {
    // first check if the schemas are equal
    SchemaComparer.assertSchemaEqual(actualDS, expectedDS, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
    val actual = if (ignoreColumnOrder) orderColumns(actualDS, expectedDS) else actualDS
    assertLargeDatasetContentEquality(actual, expectedDS, equals, orderedComparison)
  }

  def assertLargeDatasetContentEquality[T: ClassTag](actualDS: Dataset[T],
                                                     expectedDS: Dataset[T],
                                                     equals: (T, T) => Boolean,
                                                     orderedComparison: Boolean): Unit = {
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

      val unequalRDD = ds1RDD
        .zip(ds2RDD)
        .filter { case (o1: T, o2: T) => !equals(o1, o2) }
        .zipWithIndex()
        .map { case ((t1, t2), idx) => (idx, (t1, t2)) }
      val maxUnequalRowsToShow = 10
      if (!unequalRDD.isEmpty()) {
        throw DatasetContentMismatch(
          unequalRDDMessage(unequalRDD, maxUnequalRowsToShow)
        )
      }
      unequalRDD.take(maxUnequalRowsToShow)

    } finally {
      ds1.rdd.unpersist()
      ds2.rdd.unpersist()
    }
  }

  def assertApproximateDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false
  ): Unit = {
    val e = (r1: Row, r2: Row) => {
      r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, precision)
    }
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
