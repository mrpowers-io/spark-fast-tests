package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import utest._

object Person {
  def caseInsensitivePersonEquals(some: Person, other: Person): Boolean = {
    some.name.equalsIgnoreCase(other.name) && some.age == other.age
  }
}
case class Person(name: String, age: Int)
case class PrecisePerson(name: String, age: Double)

object DatasetComparerTest
    extends TestSuite
    with DatasetComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'checkDatasetEquality - {

      import spark.implicits._

      "provides a good README example" - {

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

        assertSmallDatasetEquality(sourceDF, expectedDF)
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

      "does nothing if the Datasets have the same schemas and content" - {
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
          assertLargeDatasetEquality(sourceDF, expectedDF)
        }
        val e2 = intercept[DatasetSchemaMismatch] {
          assertSmallDatasetEquality(sourceDF, expectedDF)
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
          assertLargeDatasetEquality(sourceDF, expectedDF)
        }
        val e2 = intercept[DatasetContentMismatch] {
          assertSmallDatasetEquality(sourceDF, expectedDF)
        }
      }

      "throws an error if the Dataset content is different" - {
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

      "succeeds if custom comparator returns true" - {
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
        assertLargeDatasetEquality(
          sourceDS,
          expectedDS,
          Person.caseInsensitivePersonEquals
        )
      }

      "fails if custom comparator for returns false" - {
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
          assertLargeDatasetEquality(
            sourceDS,
            expectedDS,
            Person.caseInsensitivePersonEquals
          )
        }
      }

    }

    'assertSmallDatasetEquality - {
      import spark.implicits._

      "ignores the nullable flag when making DataFrame comparisons" - {
        val sourceDF = spark.createDF(
          List(
            (1),
            (5)
          ),
          List(
            ("number", IntegerType, false)
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

        assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
      }

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

        assertSmallDatasetEquality(
          sourceDF,
          expectedDF,
          orderedComparison = false
        )
      }

      "can performed unordered Dataset comparisons" - {
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
        assertSmallDatasetEquality(
          sourceDS,
          expectedDS,
          orderedComparison = false
        )
      }

      "throws an error for unordered Dataset comparisons that don't match" - {
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
          assertSmallDatasetEquality(
            sourceDS,
            expectedDS,
            orderedComparison = false
          )
        }
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
          assertSmallDatasetEquality(
            sourceDF,
            expectedDF,
            orderedComparison = false
          )
        }
      }

    }

    'defaultSortDataset - {

      "sorts a DataFrame by the column names in alphabetical order" - {
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

    'assertApproximateDataFrameEquality - {

      "does nothing if the DataFrames have the same schemas and content" - {
        val sourceDF = spark.createDF(
          List(
            (1.2),
            (5.1),
            (null)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1.2),
            (5.1),
            (null)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }

      "throws an error if the rows are different" - {
        val sourceDF = spark.createDF(
          List(
            (100.9),
            (5.1)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1.2),
            (5.1)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        val e = intercept[DatasetContentMismatch] {
          assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
        }
      }

      "throws an error DataFrames have a different number of rows" - {
        val sourceDF = spark.createDF(
          List(
            (1.2),
            (5.1),
            (8.8)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            (1.2),
            (5.1)
          ),
          List(
            ("number", DoubleType, true)
          )
        )

        val e = intercept[DatasetCountMismatch] {
          assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
        }
      }

      //      "works with FloatType columns" - {
      //        val sourceDF = spark.createDF(
      //          List(
      //            (1.2),
      //            (5.1),
      //            (null)
      //          ), List(
      //            ("number", FloatType, true)
      //          )
      //        )
      //
      //        val expectedDF = spark.createDF(
      //          List(
      //            (1.2),
      //            (5.1),
      //            (null)
      //          ), List(
      //            ("number", FloatType, true)
      //          )
      //        )
      //
      //        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      //      }

    }

  }

}
