package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import utest._

object SchemaComparerTest extends TestSuite {

  val tests = Tests {

    'equals - {

      "returns true if the schemas are equal" - {

        val s1 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              true
            ),
            StructField(
              "mood",
              StringType,
              true
            )
          )
        )

        val s2 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              true
            ),
            StructField(
              "mood",
              StringType,
              true
            )
          )
        )

        assert(
          SchemaComparer.equals(
            s1,
            s2
          ) == true
        )

      }

      "returns false if the schemas aren't equal" - {

        val s1 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              true
            )
          )
        )

        val s2 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              true
            ),
            StructField(
              "mood",
              StringType,
              true
            )
          )
        )

        assert(
          SchemaComparer.equals(
            s1,
            s2
          ) == false
        )

      }

      "can ignore the nullable flag when determining equality" - {

        val s1 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              true
            ),
            StructField(
              "mood",
              StringType,
              true
            )
          )
        )

        val s2 = StructType(
          Seq(
            StructField(
              "something",
              StringType,
              false
            ),
            StructField(
              "mood",
              StringType,
              true
            )
          )
        )

        assert(
          SchemaComparer.equals(
            s1,
            s2,
            ignoreNullable = true
          ) == true
        )

      }

    }

  }

}
