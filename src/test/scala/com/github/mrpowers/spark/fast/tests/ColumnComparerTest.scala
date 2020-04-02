package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date
import java.sql.Timestamp

import org.scalatest.FreeSpec

class ColumnComparerTest extends FreeSpec with ColumnComparer with SparkSessionTestWrapper {

  "assertColumnEquality" - {

    "throws an easily readable error message" in {
      val sourceData = Seq(
        Row("phil", "phil"),
        Row("rashid", "rashid"),
        Row("matthew", "mateo"),
        Row("sami", "sami"),
        Row("this is something that is super crazy long", "sami"),
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
      val e2 = intercept[ColumnMismatch] {
        assertColEquality(sourceDF, "name", "expected_name")
      }
    }

    "doesn't thrown an error when the columns are equal" in {
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
      assertColEquality(sourceDF, "num", "expected_num")
    }

    "throws an error if the columns are not equal" in {
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
      val e2 = intercept[ColumnMismatch] {
        assertColEquality(sourceDF, "num", "expected_num")
      }
    }

    "throws an error if the columns are different types" in {
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
      val e2 = intercept[ColumnMismatch] {
        assertColEquality(sourceDF, "num", "word")
      }
    }

    "works properly, even when null is compared with a value" in {
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
      val e2 = intercept[ColumnMismatch] {
        assertColEquality(sourceDF, "num", "expected_num")
      }
    }

    "works for ArrayType columns" in {
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
      assertColEquality(sourceDF, "l1", "l2")
    }

    "works for nested arrays" in {
      val sourceData = Seq(
        Row(Array(Array("a"), Array("a")), Array(Array("a"), Array("a"))),
        Row(Array(Array("a", "b"), Array("a", "b")), Array(Array("a", "b"), Array("a", "b"))),
        Row(Array(Array(), Array()), Array(Array(), Array())),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("l1", ArrayType(ArrayType(StringType, true)), true),
        StructField("l2", ArrayType(ArrayType(StringType, true)), true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertColumnEquality(sourceDF, "l1", "l2")
      assertColEquality(sourceDF, "l1", "l2")
    }

    "works for computed ArrayType columns" in {
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
      assertColEquality(actualDF, "colors", "expected_colors")
    }

    "works for MapType columns" in {
      val data = Seq(
        Row(Map("good_song" -> "santeria", "bad_song" -> "doesn't exist"), Map("good_song" -> "santeria", "bad_song" -> "doesn't exist"))
      )
      val schema = List(
        StructField("m1", MapType(StringType, StringType, true), true),
        StructField("m2", MapType(StringType, StringType, true), true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema)
      )
      assertColumnEquality(df, "m1", "m2")
    }

    "throws error when MapType columns aren't equal" in {
      val data = Seq(
        Row(Map("good_song" -> "santeria", "bad_song" -> "doesn't exist"), Map("good_song" -> "what i got", "bad_song" -> "doesn't exist"))
      )
      val schema = List(
        StructField("m1", MapType(StringType, StringType, true), true),
        StructField("m2", MapType(StringType, StringType, true), true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema)
      )
      val e = intercept[ColumnMismatch] {
        assertColumnEquality(df, "m1", "m2")
      }
    }

    "works for MapType columns with deep comparisons" in {
      val data = Seq(
        Row(Map("good_song" -> Array(1, 2, 3, 4)), Map("good_song" -> Array(1, 2, 3, 4)))
      )
      val schema = List(
        StructField("m1", MapType(StringType, ArrayType(IntegerType, true), true), true),
        StructField("m2", MapType(StringType, ArrayType(IntegerType, true), true), true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema)
      )
      assertColumnEquality(df, "m1", "m2")
    }

    "throws an errors for MapType columns with deep comparisons that aren't equal" in {
      val data = Seq(
        Row(Map("good_song" -> Array(1, 2, 3, 4)), Map("good_song" -> Array(1, 2, 3, 8)))
      )
      val schema = List(
        StructField("m1", MapType(StringType, ArrayType(IntegerType, true), true), true),
        StructField("m2", MapType(StringType, ArrayType(IntegerType, true), true), true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema)
      )
      val e = intercept[ColumnMismatch] {
        assertColumnEquality(df, "m1", "m2")
      }
    }

    "works when DateType columns are equal" in {
      val sourceData = Seq(
        Row(Date.valueOf("2016-08-09"), Date.valueOf("2016-08-09")),
        Row(Date.valueOf("2019-01-01"), Date.valueOf("2019-01-01")),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("d1", DateType, true),
        StructField("d2", DateType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertColumnEquality(sourceDF, "d1", "d2")
    }

    "throws an error when DateType columns are not equal" in {
      val sourceData = Seq(
        Row(Date.valueOf("2010-07-07"), Date.valueOf("2016-08-09")),
        Row(Date.valueOf("2019-01-01"), Date.valueOf("2019-01-01")),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("d1", DateType, true),
        StructField("d2", DateType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "d1", "d2")
      }
    }

    "works when TimestampType columns are equal" in {
      val sourceData = Seq(
        Row(Timestamp.valueOf("2016-08-09 09:57:00"), Timestamp.valueOf("2016-08-09 09:57:00")),
        Row(Timestamp.valueOf("2016-04-10 09:57:00"), Timestamp.valueOf("2016-04-10 09:57:00")),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("t1", TimestampType, true),
        StructField("t2", TimestampType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertColumnEquality(sourceDF, "t1", "t2")
    }

    "throws an error when TimestampType columns are not equal" in {
      val sourceData = Seq(
        Row(Timestamp.valueOf("2010-08-09 09:57:00"), Timestamp.valueOf("2016-08-09 09:57:00")),
        Row(Timestamp.valueOf("2016-04-10 10:01:00"), Timestamp.valueOf("2016-04-10 09:57:00")),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("t1", TimestampType, true),
        StructField("t2", TimestampType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "t1", "t2")
      }
    }

    "works when ByteType columns are equal" in {
      val sourceData = Seq(
        Row(10.toByte, 10.toByte),
        Row(33.toByte, 33.toByte),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("b1", ByteType, true),
        StructField("b2", ByteType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertColumnEquality(sourceDF, "b1", "b2")
    }

    "throws an error when ByteType columns are not equal" in {
      val sourceData = Seq(
        Row(8.toByte, 10.toByte),
        Row(33.toByte, 33.toByte),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("b1", ByteType, true),
        StructField("b2", ByteType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertColumnEquality(sourceDF, "b1", "b2")
      }
    }

  }

  "assertBinaryTypeColumnEquality" - {

    "works when BinaryType columns are equal" in {
      val sourceData = Seq(
        Row(Array(10.toByte, 15.toByte), Array(10.toByte, 15.toByte)),
        Row(Array(4.toByte, 33.toByte), Array(4.toByte, 33.toByte)),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("b1", BinaryType, true),
        StructField("b2", BinaryType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertBinaryTypeColumnEquality(sourceDF, "b1", "b2")
    }

    "throws an error when BinaryType columns are not equal" in {
      val sourceData = Seq(
        Row(Array(10.toByte, 15.toByte), Array(10.toByte, 15.toByte)),
        Row(Array(4.toByte, 33.toByte), Array(4.toByte, 33.toByte)),
        Row(null, null),
        Row(Array(7.toByte, 33.toByte), Array(4.toByte, 33.toByte)),
        Row(Array(4.toByte, 33.toByte), null),
        Row(null, Array(4.toByte, 33.toByte))
      )
      val sourceSchema = List(
        StructField("b1", BinaryType, true),
        StructField("b2", BinaryType, true)
      )
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertBinaryTypeColumnEquality(sourceDF, "b1", "b2")
      }
    }

  }

  "assertDoubleTypeColumnEquality" - {

    "doesn't throw an error when two DoubleType columns are equal" in {
      val sourceData = Seq(
        Row(1.3, 1.3),
        Row(5.01, 5.0101),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("d1", DoubleType, true),
        StructField("d2", DoubleType, true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertDoubleTypeColumnEquality(df, "d1", "d2", 0.01)
    }

    "throws an error when two DoubleType columns are not equal" in {
      val sourceData = Seq(
        Row(1.3, 1.8),
        Row(5.01, 5.0101),
        Row(null, 10.0),
        Row(3.4, null),
        Row(-1.1, -1.1),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("d1", DoubleType, true),
        StructField("d2", DoubleType, true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertDoubleTypeColumnEquality(df, "d1", "d2", 0.01)
      }
    }

  }

  "assertFloatTypeColumnEquality" - {

    "doesn't throw an error when two FloatType columns are equal" in {
      val sourceData = Seq(
        Row(1.3f, 1.3f),
        Row(5.01f, 5.0101f),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("num1", FloatType, true),
        StructField("num2", FloatType, true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      assertFloatTypeColumnEquality(df, "num1", "num2", 0.01f)
    }

    "throws an error when two FloatType columns are not equal" in {
      val sourceData = Seq(
        Row(1.3f, 1.8f),
        Row(5.01f, 5.0101f),
        Row(null, 10.0f),
        Row(3.4f, null),
        Row(null, null)
      )
      val sourceSchema = List(
        StructField("d1", FloatType, true),
        StructField("d2", FloatType, true)
      )
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )
      val e = intercept[ColumnMismatch] {
        assertFloatTypeColumnEquality(df, "d1", "d2", 0.01f)
      }
    }

  }

  "assertColEquality" - {

    "throws an easily readable error message" in {
      val sourceData = Seq(
        Row("phil", "phil"),
        Row("rashid", "rashid"),
        Row("matthew", "mateo"),
        Row("sami", "sami"),
        Row("this is something that is super crazy long", "sami"),
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
        assertColEquality(sourceDF, "name", "expected_name")
      }
    }

    "doesn't thrown an error when the columns are equal" in {
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
      assertColEquality(sourceDF, "num", "expected_num")
    }

    "throws an error if the columns are not equal" in {
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
        assertColEquality(sourceDF, "num", "expected_num")
      }
    }

    "throws an error if the columns are different types" in {
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
        assertColEquality(sourceDF, "num", "word")
      }
    }

    "works properly, even when null is compared with a value" in {
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

    "works for ArrayType columns" in {
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
  }

}
