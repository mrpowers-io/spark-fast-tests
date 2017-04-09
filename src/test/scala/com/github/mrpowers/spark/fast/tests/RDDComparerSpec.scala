package com.github.mrpowers.spark.fast.tests

import org.scalatest.FunSpec

class RDDComparerSpec extends FunSpec with RDDComparer with SparkSessionTestWrapper {

  describe("contentMismatchMessage") {

    it("returns a string string to compare the expected and actual RDDs") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )

      val actualRDD = spark.sparkContext.parallelize(sourceData)

      val expectedData = List(
        ("man"),
        ("can"),
        ("pan")
      )

      val expectedRDD = spark.sparkContext.parallelize(expectedData)

      val expected = """
Actual RDD Content:
cat
dog
frog
Expected RDD Content:
man
can
pan
"""

      assert(contentMismatchMessage(actualRDD, expectedRDD) === expected)

    }

  }

  describe("#assertSmallRDDEquality") {

    it("does nothing if the RDDs have the same content") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )

      val sourceRDD = spark.sparkContext.parallelize(sourceData)

      val expectedData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )

      val expectedRDD = spark.sparkContext.parallelize(expectedData)

      assertSmallRDDEquality(sourceRDD, expectedRDD)

    }

    it("throws an error if the RDDs have different content") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )
      val sourceRDD = spark.sparkContext.parallelize(sourceData)

      val expectedData = List(
        ("mouse"),
        ("pig"),
        ("frog")
      )

      val expectedRDD = spark.sparkContext.parallelize(expectedData)

      intercept[RDDContentMismatch] {
        assertSmallRDDEquality(sourceRDD, expectedRDD)
      }

    }

  }

}
