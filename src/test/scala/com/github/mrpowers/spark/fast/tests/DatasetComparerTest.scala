package com.github.mrpowers.spark.fast.tests

import utest._

object Person {
  def caseInsensitivePersonEquals(some: Person, other: Person): Boolean = {
    some.name.equalsIgnoreCase(other.name) && some.age == other.age
  }
}
case class Person(name: String, age: Int)

object DatasetComparerTest
    extends TestSuite
    with DatasetComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'assertLargeDatasetEquality - {
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

        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

      "does nothing if the Datasets have the same schemas and content" - {
        val sourceDS = Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        ).toDS

        val expectedDS = Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        ).toDS

        assertLargeDatasetEquality(sourceDS, expectedDS)
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
          assertLargeDatasetEquality(sourceDF, expectedDF)
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
          assertLargeDatasetEquality(sourceDF, expectedDF)
        }
      }

      "returns false if the Dataset content is different" - {
        val sourceDS = Seq(
          Person("bob", 1),
          Person("alice", 5)
        ).toDS

        val expectedDS = Seq(
          Person("frank", 10),
          Person("lucy", 5)
        ).toDS

        val e = intercept[DatasetContentMismatch] {
          assertLargeDatasetEquality(sourceDS, expectedDS)
        }
      }

      "succeeds if custom comparator returns true" - {
        val sourceDS = Seq(Person("bob", 1), Person("alice", 5)).toDS
        val expectedDS = Seq(Person("Bob", 1), Person("Alice", 5)).toDS
        assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
      }

      "fails if custom comparator for returns false" - {
        val sourceDS = Seq(Person("bob", 10), Person("alice", 5)).toDS
        val expectedDS = Seq(Person("Bob", 1), Person("Alice", 5)).toDS
        val e = intercept[DatasetContentMismatch] {
          assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
        }
      }

    }

    'assertSmallDatasetEquality - {
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

        assertSmallDatasetEquality(sourceDF, expectedDF)
      }

      "does nothing true if the Datasets have the same schemas and content" - {
        val sourceDS = Seq(
          Person("bob", 1),
          Person("frank", 5)
        ).toDS

        val expectedDS = Seq(
          Person("bob", 1),
          Person("frank", 5)
        ).toDS

        assertSmallDatasetEquality(sourceDS, expectedDS)
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

        assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
      }

      "can performed unordered Dataset comparisons" - {
        val sourceDS = Seq(
          Person("bob", 1),
          Person("frank", 5)
        ).toDS

        val expectedDS = Seq(
          Person("frank", 5),
          Person("bob", 1)
        ).toDS

        assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }

      "throws an error for unordered Dataset comparisons that don't match" - {
        val sourceDS = Seq(
          Person("bob", 1),
          Person("frank", 5)
        ).toDS

        val expectedDS = Seq(
          Person("frank", 5),
          Person("bob", 1),
          Person("sadie", 2)
        ).toDS

        val e = intercept[DatasetContentMismatch] {
          assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
        }
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
          assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
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
          assertSmallDatasetEquality(sourceDF, expectedDF)
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
          assertSmallDatasetEquality(sourceDF, expectedDF)
        }
      }

      "returns false if the Dataset content is different" - {
        val sourceDS = Seq(
          Person("bob", 1),
          Person("frank", 5)
        ).toDS

        val expectedDS = Seq(
          Person("sally", 66),
          Person("sue", 54)
        ).toDS

        val e = intercept[DatasetContentMismatch] {
          assertSmallDatasetEquality(sourceDS, expectedDS)
        }
      }

    }

    'defaultSortDataset - {
      import spark.implicits._

      "sorts a DataFrame by the column names in alphabetical order" - {
        val sourceDF = Seq(
          (5, "bob"),
          (1, "phil"),
          (5, "anne")
        ).toDF("fun_level", "name")

        val actualDF = defaultSortDataset(sourceDF)

        val expectedDF = Seq(
          (1, "phil"),
          (5, "anne"),
          (5, "bob")
        ).toDF("fun_level", "name")

        assertSmallDatasetEquality(actualDF, expectedDF)
      }

    }

  }

}
