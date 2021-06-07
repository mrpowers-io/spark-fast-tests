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

  private def betterSchemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    "\nActual Schema Field | Expected Schema Field\n" + actualDS.schema
      .zipAll(
        expectedDS.schema,
        "",
        ""
      )
      .map {
        case (sf1, sf2) if sf1 == sf2 =>
          ufansi.Color.Blue(s"$sf1 | $sf2")
        case ("", sf2) =>
          ufansi.Color.Red(s"MISSING | $sf2")
        case (sf1, "") =>
          ufansi.Color.Red(s"$sf1 | MISSING")
        case (sf1, sf2) =>
          ufansi.Color.Red(s"$sf1 | $sf2")
      }
      .mkString("\n")
  }

  private def countMismatchMessage(actualCount: Long, expectedCount: Long): String = {
    s"""
Actual DataFrame Row Count: '${actualCount}'
Expected DataFrame Row Count: '${expectedCount}'
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
      .map {
        case (idx, (left, right)) =>
          ufansi.Color.Red(s"$idx | $left | $right")
      }
      .mkString("\n")
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T](actualDS: Dataset[T],
                                    expectedDS: Dataset[T],
                                    ignoreNullable: Boolean = false,
                                    ignoreColumnNames: Boolean = false,
                                    orderedComparison: Boolean = true,
                                    truncate: Int = 500): Unit = {
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames)) {
      throw DatasetSchemaMismatch(
        betterSchemaMismatchMessage(actualDS, expectedDS)
      )
    }
    if (orderedComparison) {
      val a = actualDS.collect()
      val e = expectedDS.collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(betterContentMismatchMessage(a, e, truncate))
      }
    } else {
      val a = defaultSortDataset(actualDS).collect()
      val e = defaultSortDataset(expectedDS).collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(betterContentMismatchMessage(a, e, truncate))
      }
    }
  }

  def defaultSortDataset[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.columns
    val cols     = colNames.map(col)
    ds.sort(cols: _*)
  }

  def sortPreciseColumns[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.dtypes
      .withFilter { dtype =>
        !(Seq("DoubleType", "DecimalType", "FloatType").contains(dtype._2))
      }
      .map(_._1)
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertLargeDatasetEquality[T: ClassTag](actualDS: Dataset[T],
                                              expectedDS: Dataset[T],
                                              equals: (T, T) => Boolean = naiveEquality _,
                                              ignoreNullable: Boolean = false,
                                              ignoreColumnNames: Boolean = false,
                                              orderedComparison: Boolean = true): Unit = {
    // first check if the schemas are equal
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames)) {
      throw DatasetSchemaMismatch(betterSchemaMismatchMessage(actualDS, expectedDS))
    }
    // then check if the DataFrames have the same content
    def throwIfDatasetsAreUnequal(ds1: Dataset[T], ds2: Dataset[T]) = {
      try {
        ds1.rdd.cache
        ds2.rdd.cache

        val actualCount   = ds1.rdd.count
        val expectedCount = ds2.rdd.count

        if (actualCount != expectedCount) {
          throw DatasetCountMismatch(countMismatchMessage(actualCount, expectedCount))
        }

        val expectedIndexValue: RDD[(Long, T)] = RddHelpers.zipWithIndex(ds1.rdd)
        val resultIndexValue: RDD[(Long, T)]   = RddHelpers.zipWithIndex(ds2.rdd)
        val unequalRDD = expectedIndexValue
          .join(resultIndexValue)
          .filter {
            case (idx, (o1, o2)) => !equals(o1, o2)
          }
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

    if (orderedComparison) {
      throwIfDatasetsAreUnequal(actualDS, expectedDS)
    } else {
      throwIfDatasetsAreUnequal(sortPreciseColumns(actualDS), sortPreciseColumns(expectedDS))
    }
  }

  def assertApproximateDataFrameEquality(actualDF: DataFrame,
                                         expectedDF: DataFrame,
                                         precision: Double,
                                         ignoreNullable: Boolean = false,
                                         ignoreColumnNames: Boolean = false,
                                         orderedComparison: Boolean = true): Unit = {
    val e = (r1: Row, r2: Row) => {
      r1.equals(r2) || RowComparer.areRowsEqual(r1, r2, precision)
    }
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = e,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison
    )
  }

}
