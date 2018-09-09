package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparerLike.naiveEquality
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.reflect.ClassTag

case class DatasetSchemaMismatch(smth: String) extends Exception(smth)

case class DatasetContentMismatch(smth: String) extends Exception(smth)

case class DatasetCountMismatch(smth: String) extends Exception(smth)

object DatasetComparerLike {

  def naiveEquality[T](o1: T, o2: T): Boolean = {
    o1.equals(o2)
  }

  def precisionEquality(o1: Row, o2: Row, precision: Double): Boolean = {
    o1.equals(o2) || RowComparer.areRowsEqual(
      o1,
      o2,
      precision
    )
  }
}

trait DatasetComparer {

  private def schemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual Schema:
${actualDS.schema}
Expected Schema:
${expectedDS.schema}
"""
  }

  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '${actualCount}'
Expected DataFrame Row Count: '${expectedCount}'
"""
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T](
    actualDS: Dataset[T],
    expectedDS: Dataset[T],
    ignoreNullable: Boolean = false,
    orderedComparison: Boolean = true
  ): Unit = {
    if (ignoreNullable) {
      if (!SchemaComparer.equals(
            actualDS.schema,
            expectedDS.schema,
            ignoreNullable = true
          )) {
        throw DatasetSchemaMismatch(
          schemaMismatchMessage(
            actualDS,
            expectedDS
          )
        )
      }
    } else {
      if (!actualDS.schema.equals(expectedDS.schema)) {
        throw DatasetSchemaMismatch(
          schemaMismatchMessage(
            actualDS,
            expectedDS
          )
        )
      }
    }
    if (orderedComparison) {
      collectAndAssert(
        actualDS,
        expectedDS,
        naiveEquality
      )
    } else {
      collectAndAssert(
        defaultSortDataset(actualDS),
        defaultSortDataset(expectedDS),
        naiveEquality
      )
    }
  }

  protected def collectAndAssert[T](actualDS: Dataset[T], expectedDS: Dataset[T], equals: (T, T) => Boolean): Unit = {
    val a = actualDS.collect()
    val e = expectedDS.collect()
    if (a.length != e.length) {
      throw DatasetContentMismatch(
        "Dataset have varying size " + countMismatchMessage(
          a.length,
          e.length
        )
      )
    }
    val tuples = a.zip(e)
    val matchResults = tuples
      .map(
        x => {
          equals(
            x._1,
            x._2
          )
        }
      )
    if (matchResults.contains(false)) {
      val messages = matchResults.zip(tuples).map {
        case (true, tuple) =>
          ufansi.Color.Blue(s"${tuple._1} | ${tuple._2}")
        case (false, tuple) =>
          ufansi.Color.Red(s"${tuple._1} | ${tuple._2}")
      }
      throw DatasetContentMismatch("\n" + messages.mkString("\n"))
    }
  }

  def defaultSortDataset[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.columns.sorted
    val cols     = colNames.map(col)
    ds.sort(cols: _*)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertLargeDatasetEquality[T: ClassTag](
    actualDS: Dataset[T],
    expectedDS: Dataset[T],
    equals: (T, T) => Boolean = naiveEquality _,
    ignoreSchemaCheck: Boolean = false
  ): Unit = {
    if (!ignoreSchemaCheck) {
      if (!actualDS.schema.equals(expectedDS.schema)) {
        throw DatasetSchemaMismatch(
          schemaMismatchMessage(
            actualDS,
            expectedDS
          )
        )
      }
    }
    try {
      actualDS.rdd.cache
      expectedDS.rdd.cache

      val actualCount   = actualDS.rdd.count
      val expectedCount = expectedDS.rdd.count
      if (actualCount != expectedCount) {
        throw DatasetCountMismatch(
          countMismatchMessage(
            actualCount,
            expectedCount
          )
        )
      }

      val expectedIndexValue: RDD[(Long, T)] =
        RddHelpers.zipWithIndex(actualDS.rdd)
      val resultIndexValue: RDD[(Long, T)] =
        RddHelpers.zipWithIndex(expectedDS.rdd)
      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter {
          case (idx, (o1, o2)) =>
            !equals(
              o1,
              o2
            )
        }
      val maxUnequalRowsToShow = 10
      if (!unequalRDD.isEmpty()) {
        throw DatasetContentMismatch(
          countMismatchMessage(
            actualCount,
            expectedCount
          )
        )
      }
      unequalRDD.take(maxUnequalRowsToShow)

    } finally {
      actualDS.rdd.unpersist()
      expectedDS.rdd.unpersist()
    }
  }

  def assertApproximateDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame, precision: Double): Unit = {
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      DatasetComparerLike.precisionEquality(
        _,
        _,
        precision
      )
    )
  }

}
