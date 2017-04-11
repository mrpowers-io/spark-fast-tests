package com.github.mrpowers.spark.fast.tests

import org.scalatest.FunSpec

class DataFrameComparerSpec extends FunSpec with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#assertDataFrameEquality") {

    it("does nothing true if the DataFrames have the same schemas and content") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1),
        (5)
      ).toDF("number")

      assertDataFrameEquality(sourceDF, expectedDF)

    }

    it("throws an error if the DataFrames have the different schemas") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1, "word"),
        (5, "word")
      ).toDF("number", "word")

      intercept[DataFrameSchemaMismatch] {
        assertDataFrameEquality(sourceDF, expectedDF)
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

      intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquality(sourceDF, expectedDF)
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

    it("throws an error if the DataFrames have the different schemas") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1, "word"),
        (5, "word")
      ).toDF("number", "word")

      intercept[DataFrameSchemaMismatch] {
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

      intercept[DataFrameContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }

    }

  }

}
