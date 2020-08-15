package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.functions._
import SparkSessionExt._

import org.scalatest.FreeSpec

class DataFrameComparerTest extends FreeSpec with DataFrameComparer with SparkSessionTestWrapper {

  "prints a descriptive error message if it bugs out" in {

    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(
        sourceDF,
        expectedDF
      )
    }

  }

  "checkingDataFrameEquality" - {

    "provides a descriptive error message" in {
      val someDF = spark.createDF(
        List(
          (8, "bat"),
          (64, "mouse"),
          (-27, "horse")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )
      assertLargeDataFrameEquality(someDF.sort(col("number")), someDF)
    }

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertSmallDataFrameEquality(
        sourceDF,
        expectedDF
      )
      assertLargeDataFrameEquality(
        sourceDF,
        expectedDF
      )
    }

    "throws an error if the DataFrames have different schemas" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(
          sourceDF,
          expectedDF
        )
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(
          sourceDF,
          expectedDF
        )
      }
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (10),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val e = intercept[DatasetContentMismatch] {
        assertLargeDataFrameEquality(
          sourceDF,
          expectedDF
        )
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(
          sourceDF,
          expectedDF
        )
      }
    }

  }

  "assertSmallDataFrameEquality" - {

    "can performed unordered DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1)
        ),
        List(("number", IntegerType, true))
      )
      assertSmallDataFrameEquality(
        sourceDF,
        expectedDF,
        orderedComparison = false
      )
    }

    "throws an error for unordered DataFrame comparisons that don't match" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1),
          (10)
        ),
        List(("number", IntegerType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(
          sourceDF,
          expectedDF,
          orderedComparison = false
        )
      }
    }

  }

}
