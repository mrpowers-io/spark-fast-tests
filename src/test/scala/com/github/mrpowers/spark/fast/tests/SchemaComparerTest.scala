package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import utest._

object SchemaComparerTest extends TestSuite {

  val tests = Tests {

    'equals - {

      "returns true if the schemas are equal" - {

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

        assert(
          SchemaComparer.equals(s1, s2) == true
        )

      }

      "works for single column schemas" - {

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

        assert(
          SchemaComparer.equals(s1, s2, true) == true
        )

      }

      "returns false if the schemas aren't equal" - {

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

        assert(
          SchemaComparer.equals(s1, s2) == false
        )

      }

      "can ignore the nullable flag when determining equality" - {

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

        assert(
          SchemaComparer.equals(s1, s2, ignoreNullable = true) == true
        )

      }

      "can ignore the nullable flag when determining equality in nested structures" - {

        val s1 = StructType(
          Seq(
            StructField("something", StringType, true),
            StructField("many_things",
                        StructType(
                          Seq(
                            StructField("some_value", IntegerType, true),
                            StructField("another_value", TimestampType, true)
                          )),
                        true)
          )
        )

        val s2 = StructType(
          Seq(
            StructField("something", StringType, true),
            StructField("many_things",
                        StructType(
                          Seq(
                            StructField("some_value", IntegerType, false),
                            StructField("another_value", TimestampType, false)
                          )),
                        false)
          )
        )

        assert(
          SchemaComparer.equals(s1, s2, ignoreNullable = true) == true
        )

      }

      "can ignore the column names flag when determining equality" - {

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

        assert(
          SchemaComparer.equals(s1, s2, ignoreColumnNames = true) == true
        )

      }

    }

  }

}
