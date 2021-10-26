package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import org.scalatest.FreeSpec

class SchemaComparerTest extends FreeSpec {

  "equals" - {

    "returns true if the schemas are equal" in {
      val s1 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      assert(SchemaComparer.equals(s1, s2))
    }

    "works for single column schemas" in {
      val s1 = StructType(
        Seq(
          StructField("something", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, false)
        )
      )
      assert(SchemaComparer.equals(s1, s2, true))
    }

    "returns false if the schemas aren't equal" in {
      val s1 = StructType(
        Seq(
          StructField("something", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      assert(!SchemaComparer.equals(s1, s2))
    }

    "can ignore the nullable flag when determining equality" in {
      val s1 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, false),
          StructField("mood", StringType, true)
        )
      )
      assert(SchemaComparer.equals(s1, s2, ignoreNullable = true))
    }

    "can ignore the column names flag when determining equality" in {
      val s1 = StructType(
        Seq(
          StructField("these", StringType, true),
          StructField("are", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("very", StringType, true),
          StructField("different", StringType, true)
        )
      )
      assert(SchemaComparer.equals(s1, s2, ignoreColumnNames = true))
    }

    "returns true if the schemas not in the same order and schemaOrderedComparison is set to false" in {
      val s1 = StructType(
        Seq(
          StructField("mood", StringType, true),
          StructField("something", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      assert(SchemaComparer.equals(s1, s2, schemaOrderedComparison = false))
    }

    "returns false if the schemas not in the same order and schemaOrderedComparison is set to true" in {
      val s1 = StructType(
        Seq(
          StructField("mood", StringType, true),
          StructField("something", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("mood", StringType, true)
        )
      )
      assert(!SchemaComparer.equals(s1, s2, schemaOrderedComparison = true))
    }

  }

}
