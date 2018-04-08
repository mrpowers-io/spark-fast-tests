package com.github.mrpowers.spark.fast.tests

import utest._

object DataFrameComparerTest
    extends TestSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'assertLargeDataFrameEquality - {
      import spark.implicits._

      "does nothing if the DataFrames have the same schemas and content" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (1),
          (5)
        ).toDF("number")

        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }

      "throws an error if the DataFrames have different schemas" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (1, "word"),
          (5, "word")
        ).toDF("number", "word")

        val e = intercept[DatasetSchemaMismatch] {
          assertLargeDataFrameEquality(sourceDF, expectedDF)
        }
      }

      "throws an error if the DataFrames content is different" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (10),
          (5)
        ).toDF("number")

        val e = intercept[DatasetContentMismatch] {
          assertLargeDataFrameEquality(sourceDF, expectedDF)
        }
      }

    }

    'assertSmallDataFrameEquality - {
      import spark.implicits._

      "does nothing true if the DataFrames have the same schemas and content" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (1),
          (5)
        ).toDF("number")

        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }

      "can performed unordered DataFrame comparisons" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (5),
          (1)
        ).toDF("number")

        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
      }

      "throws an error for unordered DataFrame comparisons that don't match" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (5),
          (1),
          (10)
        ).toDF("number")

        val e = intercept[DatasetContentMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
        }
      }

      "throws an error if the Datasets have the different schemas" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (1, "word"),
          (5, "word")
        ).toDF("number", "word")

        val e = intercept[DatasetSchemaMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF)
        }
      }

      "returns false if the DataFrames content is different" - {
        val sourceDF = Seq(
          (1),
          (5)
        ).toDF("number")

        val expectedDF = Seq(
          (10),
          (5)
        ).toDF("number")

        val e = intercept[DatasetContentMismatch] {
          assertSmallDataFrameEquality(sourceDF, expectedDF)
        }
      }

    }

  }

}
