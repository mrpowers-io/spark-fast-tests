package com.github.mrpowers.spark.fast.tests

import org.scalatest.Suite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
case class DatasetContentMismatch(smth: String) extends Exception(smth)

trait DatasetComparer
    extends TestSuite
    with DatasetComparerLike { self: Suite =>
}

trait DatasetComparerLike extends TestSuiteLike {

  def schemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual Schema:
${actualDS.schema}
Expected Schema:
${expectedDS.schema}
"""
  }

  def contentMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual DataFrame Content:
${DataFramePrettyPrint.showString(actualDS.toDF(), 5)}
Expected DataFrame Content:
${DataFramePrettyPrint.showString(expectedDS.toDF(), 5)}
"""
  }

  def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '${actualCount}'
Expected DataFrame Row Count: '${expectedCount}'
"""
  }

  def assertSmallDatasetEquality[T](
    actualDS: Dataset[T],
    expectedDS: Dataset[T],
    orderedComparison: Boolean = true
  ): Unit = {
    if (!actualDS.schema.equals(expectedDS.schema)) {
      throw new DatasetSchemaMismatch(schemaMismatchMessage(actualDS, expectedDS))
    }
    if (orderedComparison == true) {
      if (!actualDS.collect().sameElements(expectedDS.collect())) {
        throw new DatasetContentMismatch(contentMismatchMessage(actualDS, expectedDS))
      }
    } else {
      val actualSortedDF = defaultSortDataset(actualDS)
      val expectedSortedDF = defaultSortDataset(expectedDS)
      if (!actualSortedDF.collect().sameElements(expectedSortedDF.collect())) {
        throw new DatasetContentMismatch(contentMismatchMessage(actualSortedDF, expectedSortedDF))
      }
    }
  }

  def defaultSortDataset[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.columns.sorted
    val cols = colNames.map(col(_))
    ds.sort(cols: _*)
  }

  def assertLargeDatasetEquality(actualDS: DataFrame, expectedDS: DataFrame): Unit = {
    if (!actualDS.schema.equals(expectedDS.schema)) {
      throw new DatasetSchemaMismatch(schemaMismatchMessage(actualDS, expectedDS))
    }
    try {
      actualDS.rdd.cache
      expectedDS.rdd.cache

      val actualCount = actualDS.rdd.count
      val expectedCount = expectedDS.rdd.count
      if (actualCount != expectedCount) {
        throw new DatasetContentMismatch(countMismatchMessage(actualCount, expectedCount))
      }

      val expectedIndexValue = zipWithIndex(actualDS.rdd)
      val resultIndexValue = zipWithIndex(expectedDS.rdd)

      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter {
          case (idx, (r1, r2)) =>
            !(r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, 0.0))
        }

      val maxUnequalRowsToShow = 10
      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))

    } finally {
      actualDS.rdd.unpersist()
      expectedDS.rdd.unpersist()
    }
  }

  /**
   * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
   * Rows together regardless of if the source is different but still compare based on
   * the order.
   */
  def zipWithIndex[U](rdd: RDD[U]) = {
    rdd.zipWithIndex().map {
      case (row, idx) =>
        (idx, row)
    }
  }

}
