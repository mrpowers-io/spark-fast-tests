package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utest._

object ColumnComparerTest
    extends TestSuite
    with ColumnComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'assertColumnEquality - {

      "throws an easily readable error message" - {

        val sourceData = Seq(
          Row("phil", "phil"),
          Row("rashid", "rashid"),
          Row("matthew", "mateo"),
          Row("sami", "sami"),
          Row("li", "feng"),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("name", StringType, true),
          StructField("expected_name", StringType, true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        val e = intercept[ColumnMismatch] {
          assertColumnEquality(sourceDF, "name", "expected_name")
        }

      }

      "doesn't thrown an error when the columns are equal" - {
        val sourceData = Seq(
          Row(1, 1),
          Row(5, 5),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("num", IntegerType, true),
          StructField("expected_num", IntegerType, true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        assertColumnEquality(sourceDF, "num", "expected_num")
      }

      "throws an error if the columns aren't equal" - {
        val sourceData = Seq(
          Row(1, 3),
          Row(5, 5),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("num", IntegerType, true),
          StructField("expected_num", IntegerType, true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        val e = intercept[ColumnMismatch] {
          assertColumnEquality(sourceDF, "num", "expected_num")
        }
      }

      "throws an error if the columns are different types" - {
        val sourceData = Seq(
          Row(1, "hi"),
          Row(5, "bye"),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("num", IntegerType, true),
          StructField("word", StringType, true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        val e = intercept[ColumnMismatch] {
          assertColumnEquality(sourceDF, "num", "word")
        }
      }

      "works properly, even when null is compared with a value" - {
        val sourceData = Seq(
          Row(1, 1),
          Row(null, 5),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("num", IntegerType, true),
          StructField("expected_num", IntegerType, true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        val e = intercept[ColumnMismatch] {
          assertColumnEquality(sourceDF, "num", "expected_num")
        }
      }

      "works for ArrayType columns" - {
        val sourceData = Seq(
          Row(Array("a"), Array("a")),
          Row(Array("a", "b"), Array("a", "b")),
          Row(Array(), Array()),
          Row(null, null)
        )

        val sourceSchema = List(
          StructField("l1", ArrayType(StringType, true), true),
          StructField("l2", ArrayType(StringType, true), true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        assertColumnEquality(sourceDF, "l1", "l2")
      }

      "works for computed ArrayType columns" - {
        val sourceData = Seq(
          Row("i like blue and red", Array("blue", "red")),
          Row("you pink and blue", Array("blue", "pink")),
          Row("i like fun", Array(""))
        )

        val sourceSchema = List(
          StructField("words", StringType, true),
          StructField("expected_colors", ArrayType(StringType, true), true)
        )

        val sourceDF = spark.createDataFrame(
          spark.sparkContext.parallelize(sourceData),
          StructType(sourceSchema)
        )

        val actualDF = sourceDF.withColumn(
          "colors",
          split(
            concat_ws(
              ",",
              when(col("words").contains("blue"), "blue"),
              when(col("words").contains("red"), "red"),
              when(col("words").contains("pink"), "pink"),
              when(col("words").contains("cyan"), "cyan")
            ),
            ","
          )
        )

        assertColumnEquality(actualDF, "colors", "expected_colors")
      }

    }

  }

}
