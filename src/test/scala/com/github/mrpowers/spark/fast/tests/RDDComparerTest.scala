package com.github.mrpowers.spark.fast.tests

import org.scalatest.FreeSpec

class RDDComparerTest extends FreeSpec with RDDComparer with SparkSessionTestWrapper {

  "contentMismatchMessage" - {

    "returns a string string to compare the expected and actual RDDs" in {
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

      assert(
        contentMismatchMessage(actualRDD, expectedRDD) == expected
      )
    }

  }

  "assertSmallRDDEquality" - {

    "does nothing if the RDDs have the same content" in {
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

    "throws an error if the RDDs have different content" in {
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

      val e = intercept[RDDContentMismatch] {
        assertSmallRDDEquality(sourceRDD, expectedRDD)
      }
    }

  }

}
