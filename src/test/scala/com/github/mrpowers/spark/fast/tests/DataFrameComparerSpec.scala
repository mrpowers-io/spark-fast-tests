package com.github.mrpowers.spark.fast.tests

import org.scalatest.FunSpec

class DataFrameComparerSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#assertLargeDataFrameEquality") {

    it("does nothing if the DataFrames have the same schemas and content") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1),
        (5)
      ).toDF("number")

      assertLargeDataFrameEquality(sourceDF, expectedDF)

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
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }

    }

    it("throws an error if the DataFrames content is different") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (10),
        (5)
      ).toDF("number")

      intercept[DatasetContentMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }

    }

  }

  describe("#assertSmallDataFrameEquality") {

    it("does nothing true if the DataFrames have the same schemas and content") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1),
        (5)
      ).toDF("number")

      assertSmallDataFrameEquality(sourceDF, expectedDF)

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

      assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)

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
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
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
        assertSmallDataFrameEquality(sourceDF, expectedDF)
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
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }

    }

  }

}
