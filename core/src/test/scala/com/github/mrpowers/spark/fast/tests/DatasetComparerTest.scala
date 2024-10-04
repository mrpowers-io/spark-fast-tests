package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.ProductUtil.showProductDiff
import com.github.mrpowers.spark.fast.tests.SchemaComparer.DatasetSchemaMismatch
import com.github.mrpowers.spark.fast.tests.StringExt.StringOps
import org.apache.spark.sql.Row
import org.scalatest.freespec.AnyFreeSpec

object Person {

  def caseInsensitivePersonEquals(some: Person, other: Person): Boolean = {
    some.name.equalsIgnoreCase(other.name) && some.age == other.age
  }
}
case class Person(name: String, age: Int)
case class PrecisePerson(name: String, age: Double)

class DatasetComparerTest extends AnyFreeSpec with DatasetComparer with SparkSessionTestWrapper {

  "checkDatasetEquality" - {
    import spark.implicits._

    "provides a good README example" in {
      val sourceDS = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("juan", 5),
        Person("frank", 10),
        Person("li", 49),
        Person("lucy", 5)
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }
    }

    "Correctly mark unequal elements" in {
      val sourceDS = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("juan", 5),
        Person("frank", 10),
        Person("li", 49),
        Person("lucy", 5)
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("Person(frank,10)", "lucy")))
      assert(actualColourGroup.contains(Seq("Person(bob,1)", "alice")))
    }

    "correctly mark unequal element for Dataset[String]" in {
      import spark.implicits._
      val sourceDS = Seq("word", "StringType", "StructField(long,LongType,true,{})").toDS

      val expectedDS = List("word", "StringType", "StructField(long,LongType2,true,{})").toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("StructField(long,LongType2,true,{})")))
      assert(actualColourGroup.contains(Seq("StructField(long,LongType,true,{})")))
    }

    "correctly mark unequal element for Dataset[Seq[String]]" in {
      import spark.implicits._

      val sourceDS = Seq(
        Seq("apple", "banana", "cherry"),
        Seq("dog", "cat"),
        Seq("red", "green", "blue")
      ).toDS

      val expectedDS = Seq(
        Seq("apple", "banana2"),
        Seq("dog", "cat"),
        Seq("red", "green", "blue")
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("banana2", "MISSING")))
      assert(actualColourGroup.contains(Seq("banana", "cherry")))
    }

    "correctly mark unequal element for Dataset[Array[String]]" in {
      import spark.implicits._

      val sourceDS = Seq(
        Array("apple", "banana", "cherry"),
        Array("dog", "cat"),
        Array("red", "green", "blue")
      ).toDS

      val expectedDS = Seq(
        Array("apple", "banana2"),
        Array("dog", "cat"),
        Array("red", "green", "blue")
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("banana2", "MISSING")))
      assert(actualColourGroup.contains(Seq("banana", "cherry")))
    }

    "correctly mark unequal element for Dataset[Map[String, String]]" in {
      import spark.implicits._

      val sourceDS = Seq(
        Map("apple" -> "banana", "apple1" -> "banana1"),
        Map("apple" -> "banana", "apple1" -> "banana1")
      ).toDS

      val expectedDS = Seq(
        Map("apple" -> "banana1", "apple1" -> "banana1"),
        Map("apple" -> "banana", "apple1"  -> "banana1")
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }
      println(e)

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("(apple, banana1)")))
      assert(actualColourGroup.contains(Seq("(apple, banana)")))
    }

    "works with really long columns" in {
      val sourceDS = Seq(
        Person("juanisareallygoodguythatilikealotOK", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("juanisareallygoodguythatilikealotNOT", 5),
        Person("frank", 10),
        Person("li", 49),
        Person("lucy", 5)
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }
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

      assertSmallDatasetEquality(sourceDF, expectedDF)
      assertLargeDatasetEquality(sourceDF, expectedDF)
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

      assertSmallDatasetEquality(sourceDS, expectedDS)
      assertLargeDatasetEquality(sourceDS, expectedDS)
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

      assertLargeDatasetEquality(sourceDF, expectedDF)
      assertSmallDatasetEquality(sourceDF, expectedDF)
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
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

      intercept[DatasetSchemaMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = Seq(
        (1), (5), (7), (1), (1)
      ).toDF("number")

      val expectedDF = Seq(
        (10), (5), (3), (7), (1)
      ).toDF("number")

      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
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

      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS)
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS)
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
      assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
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
      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
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

      assertLargeDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
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
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }
    }

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

      assertLargeDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "throws an error for unordered Dataset comparisons that don't match" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("frank", 5)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("frank", 5),
          Person("bob", 1),
          Person("sadie", 2)
        )
      )

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }
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

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
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

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
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
      assertLargeDatasetEquality(sourceDF, expectedDF, ignoreColumnOrder = true)
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

      assertLargeDatasetEquality(ds1, ds2, ignoreColumnOrder = true)
      assertLargeDatasetEquality(ds2, ds1, ignoreColumnOrder = true)
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
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("word", "StringType", "StructField(long,LongType,true,{})")))
      assert(actualColourGroup.contains(Seq("float", "DoubleType", "MISSING")))
    }
  }

  "assertSmallDatasetEquality" - {
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

      assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
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
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }
    }

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
      assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "can performed unordered Dataset comparisons" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("alice", 5),
          Person("bob", 1)
        )
      )
      assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
    }

    "throws an error for unordered Dataset comparisons that don't match" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("frank", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("frank", 5),
          Person("bob", 1),
          Person("sadie", 2)
        )
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }
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
        assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
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
      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
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
      assertSmallDatasetEquality(sourceDF, expectedDF, ignoreColumnOrder = true)
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

      assertSmallDatasetEquality(ds2, ds1, ignoreColumnOrder = true)
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
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }

      val colourGroup         = e.getMessage.extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(Seq("word", "StringType", "StructField(long,LongType,true,{})")))
      assert(actualColourGroup.contains(Seq("float", "DoubleType", "MISSING")))
    }
  }

  "defaultSortDataset" - {

    "sorts a DataFrame by the column names in alphabetical order" in {
      val sourceDF = spark.createDF(
        List(
          (5, "bob"),
          (1, "phil"),
          (5, "anne")
        ),
        List(
          ("fun_level", IntegerType, true),
          ("name", StringType, true)
        )
      )
      val actualDF = defaultSortDataset(sourceDF)
      val expectedDF = spark.createDF(
        List(
          (1, "phil"),
          (5, "anne"),
          (5, "bob")
        ),
        List(
          ("fun_level", IntegerType, true),
          ("name", StringType, true)
        )
      )
      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  }

  "assertApproximateDataFrameEquality" - {

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          (1.2),
          (5.1),
          (null)
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1.2),
          (5.1),
          (null)
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
    }

    "throws an error if the rows are different" in {
      val sourceDF = spark.createDF(
        List(
          (100.9),
          (5.1)
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1.2),
          (5.1)
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
      val sourceDF = spark.createDF(
        List(
          (1.2),
          (5.1),
          (8.8)
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1.2),
          (5.1)
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetCountMismatch] {
        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "can ignore the nullable property" in {
      val sourceDF = spark.createDF(
        List(
          (1.2),
          (5.1)
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          (1.2),
          (5.1)
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreNullable = true)
    }

    "can ignore the column names" in {
      val sourceDF = spark.createDF(
        List(
          (1.2),
          (5.1),
          (null)
        ),
        List(("BLAHBLBH", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1.2),
          (5.1),
          (null)
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreColumnNames = true)
    }

    "can work with precision and unordered comparison" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996),
        ("1", "11/01/2019", 26.762499999999996)
      ).toDF("col_B", "col_C", "col_A")

      val ds2 = Seq(
        ("1", "10/01/2019", 26.762499999999946),
        ("1", "11/01/2019", 26.76249999999991)
      ).toDF("col_B", "col_C", "col_A")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison 2" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996, "A"),
        ("1", "10/01/2019", 26.762499999999996, "B")
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("1", "10/01/2019", 26.762499999999946, "A"),
        ("1", "10/01/2019", 26.76249999999991, "B")
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison on nested column" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996)),
        ("1", "11/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("1", "11/01/2019", 26.7624999999999961, Seq(26.7624999999999961, 26.7624999999999961)),
        ("1", "10/01/2019", 26.762499999999997, Seq(26.762499999999997, 26.762499999999997))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }
  }

//      "works with FloatType columns" - {
//        val sourceDF = spark.createDF(
//          List(
//            (1.2),
//            (5.1),
//            (null)
//          ),
//          List(
//            ("number", FloatType, true)
//          )
//        )
//
//        val expectedDF = spark.createDF(
//          List(
//            (1.2),
//            (5.1),
//            (null)
//          ),
//          List(
//            ("number", FloatType, true)
//          )
//        )
//
//        assertApproximateDataFrameEquality(
//          sourceDF,
//          expectedDF,
//          0.01
//        )
//      }

}
