package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{IntegerType, StringType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import utest._

object DataFrameComparerTest
    extends TestSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'checkingDataFrameEquality - {

      "does nothing if the DataFrames have the same schemas and content" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(sourceDF, expectedDF)
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }

      "throws an error if the DataFrames have different schemas" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1, "word"),
            (5, "word")
          ), List(
            ("number", IntegerType, true),
            ("word", StringType, true)
          )
        )

        val e = intercept[DatasetSchemaMismatch] {
          assertLargeDataFrameEquality(sourceDF, expectedDF)
        }
        val e2 = intercept[DatasetSchemaMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF)
        }
      }

      "throws an error if the DataFrames content is different" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )

        val e = intercept[DatasetContentMismatch] {
          assertLargeDataFrameEquality(sourceDF, expectedDF)
        }
        val e2 = intercept[DatasetContentMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF)
        }
      }

    }

    'assertSmallDataFrameEquality - {

      "can performed unordered DataFrame comparisons" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )
        val expectedDF = spark.createDF(
          List(
            (5),
            (1)
          ), List(
            ("number", IntegerType, true)
          )
        )
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
      }

      "throws an error for unordered DataFrame comparisons that don't match" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ), List(
            ("number", IntegerType, true)
          )
        )
        val expectedDF = spark.createDF(
          List(
            (5),
            (1),
            (10)
          ), List(
            ("number", IntegerType, true)
          )
        )
        val e = intercept[DatasetContentMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
        }
      }

    }

  }

}
