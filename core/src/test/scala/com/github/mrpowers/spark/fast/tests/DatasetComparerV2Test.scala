package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.SchemaComparer.DatasetSchemaMismatch
import com.github.mrpowers.spark.fast.tests.TestUtilsExt.ExceptionOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}
import org.scalatest.freespec.AnyFreeSpec

class DatasetComparerV2Test extends AnyFreeSpec with DatasetComparer {
  lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  "checkDatasetEquality" - {
    import spark.implicits._

    "can compare DataFrame" in {
      val sourceDF = spark.createDF(
        List(
          (1, "text"),
          (5, "text")
        ),
        List(("number", IntegerType, true), ("text", StringType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1, "text"),
          (5, "text")
        ),
        List(("number", IntegerType, true), ("text", StringType, true))
      )

      assertLargeDatasetEqualityV2(sourceDF, expectedDF)
    }

    "can compare Dataset[Array[_]]" in {
      val sourceDS = Seq(
        Array("apple", "banana", "cherry"),
        Array("dog", "cat"),
        Array("red", "green", "blue")
      ).toDS

      val expectedDS = Seq(
        Array("apple", "banana", "cherry"),
        Array("dog", "cat"),
        Array("red", "green", "blue")
      ).toDS

      assertLargeDatasetEqualityV2(sourceDS, expectedDS, equals = (a1: Array[String], a2: Array[String]) => a1.mkString == a2.mkString)
    }

    "can compare Dataset[Map[_]]" in {
      val sourceDS = Seq(
        Map("apple" -> "banana", "apple1" -> "banana1"),
        Map("apple" -> "banana", "apple1" -> "banana1")
      ).toDS

      val expectedDS = Seq(
        Map("apple" -> "banana", "apple1" -> "banana1"),
        Map("apple" -> "banana", "apple1" -> "banana1")
      ).toDS

      assertLargeDatasetEqualityV2(sourceDS, expectedDS)
    }

    "does nothing if the Datasets have the same schemas and content" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      assertLargeDatasetEqualityV2(sourceDS, expectedDS)
    }

    "works with DataFrames that have ArrayType columns" in {
      val sourceDF = spark.createDF(
        List(
          (1, Array("word1", "blah")),
          (5, Array("hi", "there"))
        ),
        List(
          ("number", IntegerType, true),
          ("words", ArrayType(StringType, true), true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, Array("word1", "blah")),
          (5, Array("hi", "there"))
        ),
        List(
          ("number", IntegerType, true),
          ("words", ArrayType(StringType, true), true)
        )
      )

      assertLargeDatasetEqualityV2(sourceDF, expectedDF)
    }

    "throws an error if the DataFrames have different schemas" in {
      val nestedSchema = StructType(
        Seq(
          StructField(
            "attributes",
            StructType(
              Seq(
                StructField("PostCode", IntegerType, nullable = true)
              )
            ),
            nullable = true
          )
        )
      )

      val nestedSchema2 = StructType(
        Seq(
          StructField(
            "attributes",
            StructType(
              Seq(
                StructField("PostCode", StringType, nullable = true)
              )
            ),
            nullable = true
          )
        )
      )

      val sourceDF = spark.createDF(
        List(
          (1, 2.0, null),
          (5, 3.0, null)
        ),
        List(
          ("number", IntegerType, true),
          ("float", DoubleType, true),
          ("nestedField", nestedSchema, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word", null, 1L),
          (5, "word", null, 2L)
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true),
          ("nestedField", nestedSchema2, true),
          ("long", LongType, true)
        )
      )

      intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEqualityV2(sourceDF, expectedDF)
      }
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = Seq(
        (1), (5), (7), (1), (1)
      ).toDF("number")

      val expectedDF = Seq(
        (10), (5), (3), (7), (1)
      ).toDF("number")

      intercept[DatasetContentMismatch] {
        assertLargeDatasetEqualityV2(sourceDF, expectedDF)
      }
    }

    "throws an error if the Dataset content is different" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Frank", 10),
          Person("Lucy", 5)
        )
      )

      intercept[DatasetContentMismatch] {
        assertLargeDatasetEqualityV2(sourceDS, expectedDS)
      }
    }

    "succeeds if custom comparator returns true" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Bob", 1),
          Person("Alice", 5)
        )
      )
      assertLargeDatasetEqualityV2(sourceDS, expectedDS, equals = Person.caseInsensitivePersonEquals)
    }

    "fails if custom comparator for returns false" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 10),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Bob", 1),
          Person("Alice", 5)
        )
      )

      intercept[DatasetContentMismatch] {
        assertLargeDatasetEqualityV2(sourceDS, expectedDS, equals = Person.caseInsensitivePersonEquals)
      }
    }

  }

  "assertLargeDatasetEquality" - {
    import spark.implicits._

    "ignores the nullable flag when making DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, false))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertLargeDatasetEqualityV2(sourceDF, expectedDF, ignoreNullable = true)
    }

    "should not ignore nullable if ignoreNullable is false" in {

      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, false))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEqualityV2(sourceDF, expectedDF)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
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
          (5),
          (10)
        ),
        List(("number", IntegerType, true))
      )

      intercept[DatasetCountMismatch] {
        assertLargeDatasetEqualityV2(sourceDF, expectedDF)
      }
    }

    "can performed DataFrame comparisons with unordered column" in {
      val sourceDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )
      val expectedDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )
      assertLargeDatasetEqualityV2(sourceDF, expectedDF, ignoreColumnOrder = true)
    }

    "can performed Dataset comparisons with unordered column" in {
      val ds1 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val ds2 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS.select("age", "name").as(ds1.encoder)

      assertLargeDatasetEqualityV2(ds2, ds1, ignoreColumnOrder = true)
    }

    "correctly mark unequal schema field" in {
      val sourceDF = spark.createDF(
        List(
          (1, 2.0),
          (5, 3.0)
        ),
        List(
          ("number", IntegerType, true),
          ("float", DoubleType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word", 1L),
          (5, "word", 2L)
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true),
          ("long", LongType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEqualityV2(sourceDF, expectedDF)
      }

      e.assertColorDiff(Seq("float", "DoubleType", "MISSING"), Seq("word", "StringType", "StructField(long,LongType,true,{})"))
    }

    "can performed Dataset comparisons and ignore metadata" in {
      val ds1 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS
        .withColumn("name", col("name").as("name", new MetadataBuilder().putString("description", "name of the person").build()))
        .as[Person]

      val ds2 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS
        .withColumn("name", col("name").as("name", new MetadataBuilder().putString("description", "name of the individual").build()))
        .as[Person]

      assertLargeDatasetEqualityV2(ds2, ds1)
    }

    "can performed Dataset comparisons and compare metadata" in {
      val ds1 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS
        .withColumn("name", col("name").as("name", new MetadataBuilder().putString("description", "name of the person").build()))
        .as[Person]

      val ds2 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS
        .withColumn("name", col("name").as("name", new MetadataBuilder().putString("description", "name of the individual").build()))
        .as[Person]

      intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEqualityV2(ds2, ds1, ignoreMetadata = false)
      }
    }
  }
}
