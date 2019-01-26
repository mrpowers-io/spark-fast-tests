package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparerLike.naiveEquality
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag

case class DatasetSchemaMismatch(smth: String)  extends Exception(smth)
case class DatasetContentMismatch(smth: String) extends Exception(smth)
case class DatasetCountMismatch(smth: String)   extends Exception(smth)

object DatasetComparerLike {

  def naiveEquality[T](o1: T, o2: T): Boolean = {
    o1.equals(o2)
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

  private def betterContentMismatchMessage[T](a: Array[T], e: Array[T]): String = {
    "\n" + a
      .zipAll(
        e,
        "",
        ""
      )
      .map {
        case (r1, r2) =>
          if (r1.equals(r2)) {
            ufansi.Color.Blue(s"$r1 | $r2")
          } else {
            ufansi.Color.Red(s"$r1 | $r2")
          }
      }
      .mkString("\n")
  }

  private def basicMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual DataFrame Content:
${DataFramePrettyPrint.showString(
      actualDS.toDF(),
      10
    )}
Expected DataFrame Content:
${DataFramePrettyPrint.showString(
      expectedDS.toDF(),
      10
    )}
"""
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T](actualDS: Dataset[T], expectedDS: Dataset[T], ignoreNullable: Boolean = false, ignoreColumnNames: Boolean = false, orderedComparison: Boolean = true): Unit = {
    if (!SchemaComparer.equals(
          actualDS.schema,
          expectedDS.schema,
          ignoreNullable,
          ignoreColumnNames
        )) {
      throw DatasetSchemaMismatch(
        schemaMismatchMessage(
          actualDS,
          expectedDS
        )
      )
    }
    if (orderedComparison) {
      val a = actualDS.collect()
      val e = expectedDS.collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(
          betterContentMismatchMessage(
            a,
            e
          )
        )
      }
    } else {
      val a = defaultSortDataset(actualDS).collect()
      val e = defaultSortDataset(expectedDS).collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(
          betterContentMismatchMessage(
            a,
            e
          )
        )
      }
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
  def assertLargeDatasetEquality[T: ClassTag](actualDS: Dataset[T], expectedDS: Dataset[T], equals: (T, T) => Boolean = naiveEquality _): Unit = {
    if (!actualDS.schema.equals(expectedDS.schema)) {
      throw DatasetSchemaMismatch(
        schemaMismatchMessage(
          actualDS,
          expectedDS
        )
      )
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
      equals = (r1: Row, r2: Row) => {
        r1.equals(r2) || RowComparer.areRowsEqual(
          r1,
          r2,
          precision
        )
      }
    )
  }

}
