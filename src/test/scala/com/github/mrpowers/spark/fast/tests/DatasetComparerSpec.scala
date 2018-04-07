package com.github.mrpowers.spark.fast.tests

import org.scalatest.FunSpec

object Person {
  def caseInsensitivePersonEquals(some: Person, other: Person): Boolean = {
    some.name.equalsIgnoreCase(other.name) && some.age == other.age
  }
}
case class Person(name: String, age: Int)

class DatasetComparerSpec
    extends FunSpec
    with DatasetComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#assertLargeDatasetEquality") {

    it("does nothing if the DataFrames have the same schemas and content") {

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

    it("does nothing if the Datasets have the same schemas and content") {

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

    it("throws an error if the DataFrames have different schemas") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1, "word"),
        (5, "word")
      ).toDF("number", "word")

      intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

    }

    it("returns false if the DataFrames content is different") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (10),
        (5)
      ).toDF("number")

      intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }

    }

    it("returns false if the Dataset content is different") {

      val sourceDS = Seq(
        Person("bob", 1),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("frank", 10),
        Person("lucy", 5)
      ).toDS

      intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS)
      }

    }

    it("succeeds if custom comparator returns true") {
      val sourceDS = Seq(Person("bob", 1), Person("alice", 5)).toDS
      val expectedDS = Seq(Person("Bob", 1), Person("Alice", 5)).toDS
      assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
    }

    it("fails if custom comparator for returns false") {
      val sourceDS = Seq(Person("bob", 10), Person("alice", 5)).toDS
      val expectedDS = Seq(Person("Bob", 1), Person("Alice", 5)).toDS
      intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
      }
    }

  }

  describe("#assertSmallDatasetEquality") {

    it("does nothing true if the DataFrames have the same schemas and content") {

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

    it("does nothing true if the Datasets have the same schemas and content") {

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

    it("can performed unordered DataFrame comparisons") {

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

    it("can performed unordered Dataset comparisons") {

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

    it("throws an error for unordered Dataset comparisons that don't match") {

      val sourceDS = Seq(
        Person("bob", 1),
        Person("frank", 5)
      ).toDS

      val expectedDS = Seq(
        Person("frank", 5),
        Person("bob", 1),
        Person("sadie", 2)
      ).toDS

      intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }

    }

    it("throws an error for unordered DataFrame comparisons that don't match") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (5),
        (1),
        (10)
      ).toDF("number")

      intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
      }

    }

    it("throws an error if the Datasets have the different schemas") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1, "word"),
        (5, "word")
      ).toDF("number", "word")

      intercept[DatasetSchemaMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }

    }

    it("returns false if the DataFrames content is different") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (10),
        (5)
      ).toDF("number")

      intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }

    }

    it("returns false if the Dataset content is different") {

      val sourceDS = Seq(
        Person("bob", 1),
        Person("frank", 5)
      ).toDS

      val expectedDS = Seq(
        Person("sally", 66),
        Person("sue", 54)
      ).toDS

      intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

    }
  }

  describe("#defaultSortDataset") {

    it("sorts a DataFrame by the column names in alphabetical order") {

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
