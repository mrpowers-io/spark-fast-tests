package com.github.mrpowers.spark.fast.tests

import org.scalatest.Suite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

case class DataFrameSchemaMismatch(smth: String) extends Exception(smth)
case class DataFrameContentMismatch(smth: String) extends Exception(smth)

trait DataFrameComparer
    extends TestSuite
    with DataFrameComparerLike { self: Suite =>
}

trait DataFrameComparerLike extends TestSuiteLike {

  def schemaMismatchMessage[T](actualDF: Dataset[T], expectedDF: Dataset[T]): String = {
    s"""
Actual Schema:
${actualDF.schema}
Expected Schema:
${expectedDF.schema}
"""
  }

  def contentMismatchMessage[T](actualDF: Dataset[T], expectedDF: Dataset[T]): String = {
    s"""
Actual DataFrame Content:
${DataFramePrettyPrint.showString(actualDF.toDF(), 5)}
Expected DataFrame Content:
${DataFramePrettyPrint.showString(expectedDF.toDF(), 5)}
"""
  }

  def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '${actualCount}'
Expected DataFrame Row Count: '${expectedCount}'
"""
  }

  def assertSmallDataFrameEquality[T](actualDF: Dataset[T], expectedDF: Dataset[T], orderedComparison: Boolean = true): Unit = {
    if (!actualDF.schema.equals(expectedDF.schema)) {
      throw new DataFrameSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
    }
    if (orderedComparison == true) {
      if (!actualDF.collect().sameElements(expectedDF.collect())) {
        throw new DataFrameContentMismatch(contentMismatchMessage(actualDF, expectedDF))
      }
    } else {
      val actualSortedDF = defaultSortDataFrame(actualDF)
      val expectedSortedDF = defaultSortDataFrame(expectedDF)
      if (!actualSortedDF.collect().sameElements(expectedSortedDF.collect())) {
        throw new DataFrameContentMismatch(contentMismatchMessage(actualSortedDF, expectedSortedDF))
      }
    }
  }

  def defaultSortDataFrame[T](df: Dataset[T]): Dataset[T] = {
    val colNames = df.columns.sorted
    val cols = colNames.map(col(_))
    df.sort(cols: _*)
  }

  def assertLargeDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    if (!actualDF.schema.equals(expectedDF.schema)) {
      throw new DataFrameSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
    }
    try {
      actualDF.rdd.cache
      expectedDF.rdd.cache

      val actualCount = actualDF.rdd.count
      val expectedCount = expectedDF.rdd.count
      if (actualCount != expectedCount) {
        throw new DataFrameContentMismatch(countMismatchMessage(actualCount, expectedCount))
      }

      val expectedIndexValue = zipWithIndex(actualDF.rdd)
      val resultIndexValue = zipWithIndex(expectedDF.rdd)

      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter {
          case (idx, (r1, r2)) =>
            !(r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, 0.0))
        }

      val maxUnequalRowsToShow = 10
      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))

    } finally {
      actualDF.rdd.unpersist()
      expectedDF.rdd.unpersist()
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
