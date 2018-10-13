package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import utest._

object DataFrameComparerTest extends TestSuite with DataFrameComparer with SparkSessionTestWrapper {

  val tests = Tests {

    "prints a descriptive error message if it bugs out" - {

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

    'checkingDataFrameEquality - {

      "does nothing if the DataFrames have the same schemas and content" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
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

      "throws an error if the DataFrames have different schemas" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
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

      "throws an error if the DataFrames content is different" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
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

    'assertSmallDataFrameEquality - {

      "can performed unordered DataFrame comparisons" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
        )
        val expectedDF = spark.createDF(
          List(
            (5),
            (1)
          ),
          List(
            ("number", IntegerType, true)
          )
        )
        assertSmallDataFrameEquality(
          sourceDF,
          expectedDF,
          orderedComparison = false
        )
      }

      "throws an error for unordered DataFrame comparisons that don't match" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, true)
          )
        )
        val expectedDF = spark.createDF(
          List(
            (5),
            (1),
            (10)
          ),
          List(
            ("number", IntegerType, true)
          )
        )
        val e = intercept[DatasetContentMismatch] {
          assertSmallDataFrameEquality(
            sourceDF,
            expectedDF,
            orderedComparison = false
          )
        }
      }

      "works with DataFrames that have StructType columns" - {

        val df1Data = Seq(
          Row(
            "darla",
            7,
            8,
            struct("hi")
          ),
          Row(
            "max",
            42,
            8,
            struct("bye")
          )
        )

        val df1Schema = Array(
          StructField(
            "name",
            StringType,
            true
          ),
          StructField(
            "age",
            IntegerType,
            true
          ),
          StructField(
            "some_number",
            IntegerType,
            true
          ),
          StructField(
            "something",
            StructType(
              Array(
                StructField(
                  "word",
                  StringType,
                  true
                )
              )
            )
          )
        )

        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(df1Data),
          StructType(df1Schema)
        )

        val df2Data = Seq(
          Row(
            "darla",
            7,
            8,
            struct("hi")
          ),
          Row(
            "max",
            42,
            8,
            struct("bye")
          )
        )

        val df2Schema = Array(
          StructField(
            "name",
            StringType,
            true
          ),
          StructField(
            "age",
            IntegerType,
            true
          ),
          StructField(
            "some_number",
            IntegerType,
            true
          ),
          StructField(
            "something",
            StructType(
              Array(
                StructField(
                  "word",
                  StringType,
                  true
                )
              )
            )
          )
        )

        val df2 = spark.createDataFrame(
          spark.sparkContext.parallelize(df1Data),
          StructType(df1Schema)
        )

        assertSmallDataFrameEquality(
          df1,
          df2,
          orderedComparison = false
        )

      }

    }

  }

}
