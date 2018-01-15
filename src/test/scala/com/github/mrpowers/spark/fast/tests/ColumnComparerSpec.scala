package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class ColumnComparerSpec
    extends FunSpec
    with ColumnComparer
    with SparkSessionTestWrapper {

  describe("#assertColumnEquality") {

    it("doesn't do anything if all the column values are equal") {

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

    it("throws an error if the columns aren't equal") {

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

      intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "num", "expected_num")
      }

    }

    it("throws an error if the columns are different types") {

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

      intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "num", "word")
      }

    }

    it("works properly, even when null is compared with a value") {

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

      intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "num", "expected_num")
      }

    }

    it("works for ArrayType columns") {

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

    it("works for computed ArrayType columns") {

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

      //      actualDF.show(truncate = false)

      assertColumnEquality(actualDF, "colors", "expected_colors")

    }

  }

}
