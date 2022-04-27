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

    "can ignore the nullable flag when determining equality on complex data types" in {
      val s1 = StructType(
        Seq(
          StructField("something", StringType, true),
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = false), true)
                )
              )),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, false),
          StructField("array", ArrayType(StringType, containsNull = false), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = true), true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = true), true)
                )
              )),
            false
          )
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

  }

}
