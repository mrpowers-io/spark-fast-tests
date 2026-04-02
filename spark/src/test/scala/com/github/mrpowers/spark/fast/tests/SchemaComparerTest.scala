package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api.DatasetSchemaMismatch
import org.apache.spark.sql.types._
import org.scalatest.freespec.AnyFreeSpec

class SchemaComparerTest extends AnyFreeSpec {

  "assertSchemaEqual" - {
    "does not throw when schemas are equal" in {
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
      SchemaComparer.assertSchemaEqual(s1, s2)
    }

    "works for single column schemas with ignoreNullable" in {
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
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

    "throws when schemas have different number of fields" in {
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
      intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2)
      }
    }

    "does not throw when ignoring nullable flag" in {
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
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

    "throws when nullable differs and ignoreNullable is false" in {
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
      intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = false)
      }
    }

    "does not throw when ignoring nullable flag on complex data types" in {
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
              )
            ),
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
              )
            ),
            false
          )
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

    "throws when nullable differs on complex data types and ignoreNullable is false" in {
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
              )
            ),
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
              )
            ),
            false
          )
        )
      )
      intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = false)
      }
    }

    "does not throw when ignoring column names" in {
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
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnNames = true, ignoreColumnOrder = false)
    }

    "does not throw when ignoring column order" in {
      val s1 = StructType(
        Seq(
          StructField("these", StringType, true),
          StructField("are", StringType, true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("are", StringType, true),
          StructField("these", StringType, true)
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = true)
    }

    "does not throw when ignoring column order on complex types" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField("something", StringType, false)
                )
              )
            ),
            true
          )
        )
      )
      val s2 = StructType(
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
              )
            ),
            true
          )
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = true)
    }

    "display schema diff as tree with different depth with ignoreColumnOrder = false" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(ArrayType(StringType, containsNull = true)), true),
          StructField("field2", ArrayType(ArrayType(StringType, containsNull = true)), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField("something", StringType, false),
                  StructField(
                    "something2",
                    StructType(
                      Seq(
                        StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                        StructField("something2", StringType, false)
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(ArrayType(StringType, containsNull = true)), true),
          StructField("field2", StringType, true),
          StructField("something", StringType, true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField(
                    "something3",
                    StructType(
                      Seq(
                        StructField("mood3", ArrayType(StringType, containsNull = false), true)
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          ),
          StructField("norma2", StringType, false)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }
      val expectedMessage = """Diffs
      |
      |Actual Schema                                               Expected Schema
      |\u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m                       \u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31mmood\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m                     \u001b[90m|    |--\u001b[39m \u001b[32msomething\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31msomething\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = false)\u001b[39m              \u001b[90m|    |--\u001b[39m \u001b[32mmood\u001b[39m : \u001b[32marray\u001b[39m \u001b[32m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m             \u001b[90m|    |--\u001b[39m \u001b[32msomething3\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[31mmood2\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m               \u001b[90m|    |    |--\u001b[39m \u001b[32mmood3\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[31mdouble\u001b[39m \u001b[90m(nullable = false)\u001b[39m      \u001b[90m|    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[32mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[31m|    |    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = false)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[90mfield2\u001b[39m : \u001b[31marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                        \u001b[90m|--\u001b[39m \u001b[90mfield2\u001b[39m : \u001b[32mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                         \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(containsNull = true)\u001b[39m              \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(containsNull = true)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m            \u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[31mmap\u001b[39m : \u001b[31mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m                             \u001b[90m|--\u001b[39m \u001b[32msomething\u001b[39m : \u001b[32mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[31msomething\u001b[39m : \u001b[31mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                    \u001b[90m|--\u001b[39m \u001b[32mmap\u001b[39m : \u001b[32mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |                                                            \u001b[32m|--\u001b[39m \u001b[32mnorma2\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff as tree with different depth with ignoreColumnOrder" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField("something", StringType, false),
                  StructField(
                    "something2",
                    StructType(
                      Seq(
                        StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                        StructField("something2", StringType, false)
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("something", StringType, true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField(
                    "something3",
                    StructType(
                      Seq(
                        StructField("mood3", ArrayType(StringType, containsNull = false), true)
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          ),
          StructField("norma2", StringType, false)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = true, outputFormat = SchemaDiffOutputFormat.Tree)
      }
      val expectedMessage = """Diffs
                              |
                              |Actual Schema                                               Expected Schema
                              |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                         \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                 \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|--\u001b[39m \u001b[90mmap\u001b[39m : \u001b[90mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m                             \u001b[90m|--\u001b[39m \u001b[90mmap\u001b[39m : \u001b[90mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90mkey\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                     \u001b[90m|    |--\u001b[39m \u001b[90mkey\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90mvalue\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                   \u001b[90m|    |--\u001b[39m \u001b[90mvalue\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                    \u001b[90m|--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m                       \u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                     \u001b[90m|    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m           \u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m              \u001b[90m|    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
                              |\u001b[31m|    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstruct\u001b[39m \u001b[31m(nullable = false)\u001b[39m
                              |\u001b[31m|    |    |--\u001b[39m \u001b[31mmood2\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m
                              |\u001b[31m|    |    |    |--\u001b[39m \u001b[31melement\u001b[39m : \u001b[31mdouble\u001b[39m \u001b[31m(nullable = false)\u001b[39m
                              |\u001b[31m|    |    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = false)\u001b[39m
                              |                                                            \u001b[32m|    |--\u001b[39m \u001b[32msomething3\u001b[39m : \u001b[32mstruct\u001b[39m \u001b[32m(nullable = false)\u001b[39m
                              |                                                            \u001b[32m|    |    |--\u001b[39m \u001b[32mmood3\u001b[39m : \u001b[32marray\u001b[39m \u001b[32m(nullable = true)\u001b[39m
                              |                                                            \u001b[32m|    |    |    |--\u001b[39m \u001b[32melement\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
                              |                                                            \u001b[32m|--\u001b[39m \u001b[32mnorma2\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
                              |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for tree with array of struct" in {
      val s1 = StructType(
        Seq(
          StructField(
            "array",
            ArrayType(StructType(Seq(StructField("arrayChild1", StringType), StructField("arrayChild3", StringType))), containsNull = true),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField(
            "array",
            ArrayType(StructType(Seq(StructField("arrayChild2", IntegerType), StructField("arrayChild4", StringType))), containsNull = false),
            true
          )
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }

      val expectedMessage = """Diffs
      |
      |Actual Schema                                             Expected Schema
      |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                       \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[31m(nullable = true)\u001b[39m               \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[31marrayChild1\u001b[39m : \u001b[31mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m      \u001b[90m|    |    |--\u001b[39m \u001b[32marrayChild2\u001b[39m : \u001b[32minteger\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[31marrayChild3\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m      \u001b[90m|    |    |--\u001b[39m \u001b[32marrayChild4\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for tree with array of array of struct" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(ArrayType(StructType(Seq(StructField("arrayChild1", StringType))), containsNull = true)))
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(ArrayType(StructType(Seq(StructField("arrayChild2", IntegerType))), containsNull = false)))
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }

      val expectedMessage = """Diffs
      |
      |Actual Schema                                                  Expected Schema
      |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                            \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90marray\u001b[39m \u001b[31m(containsNull = true)\u001b[39m                 \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90marray\u001b[39m \u001b[32m(containsNull = false)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[31m(nullable = true)\u001b[39m               \u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[31marrayChild1\u001b[39m : \u001b[31mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m      \u001b[90m|    |    |    |--\u001b[39m \u001b[32marrayChild2\u001b[39m : \u001b[32minteger\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for tree with array of simple type" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(IntegerType, containsNull = true), true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }

      val expectedMessage = """Diffs
      |
      |Actual Schema                                    Expected Schema
      |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m              \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[31mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m      \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[32minteger\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for tree with MapType" in {
      val s1 = StructType(
        Seq(
          StructField("userMap", MapType(StringType, IntegerType, valueContainsNull = true), true)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("userMap", MapType(StringType, StringType, valueContainsNull = false), true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }

      val expectedMessage = """Diffs
                              |
                              |Actual Schema                                   Expected Schema
                              |\u001b[90m|--\u001b[39m \u001b[90muserMap\u001b[39m : \u001b[90mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m             \u001b[90m|--\u001b[39m \u001b[90muserMap\u001b[39m : \u001b[90mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90mkey\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m         \u001b[90m|    |--\u001b[39m \u001b[90mkey\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |\u001b[90m|    |--\u001b[39m \u001b[90mvalue\u001b[39m : \u001b[31minteger\u001b[39m \u001b[90m(nullable = true)\u001b[39m      \u001b[90m|    |--\u001b[39m \u001b[90mvalue\u001b[39m : \u001b[32mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
                              |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for wide tree" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField("something", StringType, false),
                  StructField(
                    "something2",
                    StructType(
                      Seq(
                        StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                        StructField(
                          "something2",
                          StructType(
                            Seq(
                              StructField("mood", ArrayType(StringType, containsNull = false), true),
                              StructField("something", StringType, false),
                              StructField(
                                "something2",
                                StructType(
                                  Seq(
                                    StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                                    StructField("something2", StringType, false)
                                  )
                                ),
                                false
                              )
                            )
                          ),
                          false
                        )
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("something", StringType, true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField(
                    "something3",
                    StructType(
                      Seq(
                        StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                        StructField(
                          "something2",
                          StructType(
                            Seq(
                              StructField("mood", ArrayType(StringType, containsNull = false), true),
                              StructField("something", StringType, false),
                              StructField(
                                "something2",
                                StructType(
                                  Seq(
                                    StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                                    StructField("something2", StringType, false)
                                  )
                                ),
                                false
                              )
                            )
                          ),
                          false
                        )
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          ),
          StructField("norma2", StringType, false)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = false, outputFormat = SchemaDiffOutputFormat.Tree)
      }
      val expectedMessage = """Diffs
      |
      |Actual Schema                                                         Expected Schema
      |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                                   \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                           \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[31mmap\u001b[39m : \u001b[31mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m                                       \u001b[90m|--\u001b[39m \u001b[32msomething\u001b[39m : \u001b[32mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[31msomething\u001b[39m : \u001b[31mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                              \u001b[90m|--\u001b[39m \u001b[32mmap\u001b[39m : \u001b[32mmap\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m                                 \u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31mmood\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m                               \u001b[90m|    |--\u001b[39m \u001b[32msomething\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31msomething\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = false)\u001b[39m                        \u001b[90m|    |--\u001b[39m \u001b[32mmood\u001b[39m : \u001b[32marray\u001b[39m \u001b[32m(nullable = true)\u001b[39m
      |\u001b[90m|    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m                       \u001b[90m|    |--\u001b[39m \u001b[32msomething3\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[90mmood2\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                         \u001b[90m|    |    |--\u001b[39m \u001b[90mmood2\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mdouble\u001b[39m \u001b[90m(nullable = false)\u001b[39m                \u001b[90m|    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mdouble\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m                  \u001b[90m|    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                     \u001b[90m|    |    |    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m           \u001b[90m|    |    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m              \u001b[90m|    |    |    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m             \u001b[90m|    |    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |    |--\u001b[39m \u001b[90mmood2\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m               \u001b[90m|    |    |    |    |--\u001b[39m \u001b[90mmood2\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
      |\u001b[90m|    |    |    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mdouble\u001b[39m \u001b[90m(nullable = false)\u001b[39m      \u001b[90m|    |    |    |    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mdouble\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |\u001b[90m|    |    |    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m        \u001b[90m|    |    |    |    |--\u001b[39m \u001b[90msomething2\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
      |                                                                      \u001b[32m|--\u001b[39m \u001b[32mnorma2\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
      |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }

    "display schema diff for wide tree with ignoreColumnOrder" in {
      val s1 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField("something", StringType, false),
                  StructField(
                    "something2",
                    StructType(
                      Seq(
                        StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                        StructField(
                          "something2",
                          StructType(
                            Seq(
                              StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                              StructField(
                                "something2",
                                StructType(
                                  Seq(
                                    StructField("mood2", ArrayType(DoubleType, containsNull = false), true),
                                    StructField("something2", StringType, false)
                                  )
                                ),
                                false
                              )
                            )
                          ),
                          false
                        )
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("array", ArrayType(StringType, containsNull = true), true),
          StructField("something", StringType, true),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, false),
                  StructField("mood", ArrayType(StringType, containsNull = false), true),
                  StructField(
                    "something3",
                    StructType(
                      Seq(
                        StructField("mood3", ArrayType(StringType, containsNull = false), true)
                      )
                    ),
                    false
                  )
                )
              )
            ),
            true
          )
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        SchemaComparer.assertSchemaEqual(s1, s2, ignoreColumnOrder = true, outputFormat = SchemaDiffOutputFormat.Tree)
      }

      val expectedMessage = """Diffs
          |
          |Actual Schema                                                         Expected Schema
          |\u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                                   \u001b[90m|--\u001b[39m \u001b[90marray\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
          |\u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                           \u001b[90m|    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
          |\u001b[31m|--\u001b[39m \u001b[31mmap\u001b[39m : \u001b[31mmap\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[31m|    |--\u001b[39m \u001b[31mkey\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[31m|    |--\u001b[39m \u001b[31mvalue\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[90m|--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m                              \u001b[90m|--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = true)\u001b[39m
          |\u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m                                 \u001b[90m|--\u001b[39m \u001b[90mstruct\u001b[39m : \u001b[90mstruct\u001b[39m \u001b[90m(nullable = true)\u001b[39m
          |\u001b[90m|    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m                               \u001b[90m|    |--\u001b[39m \u001b[90mmood\u001b[39m : \u001b[90marray\u001b[39m \u001b[90m(nullable = true)\u001b[39m
          |\u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m                     \u001b[90m|    |    |--\u001b[39m \u001b[90melement\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
          |\u001b[90m|    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m                        \u001b[90m|    |--\u001b[39m \u001b[90msomething\u001b[39m : \u001b[90mstring\u001b[39m \u001b[90m(nullable = false)\u001b[39m
          |\u001b[31m|    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstruct\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |--\u001b[39m \u001b[31mmood2\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[31m|    |    |    |--\u001b[39m \u001b[31melement\u001b[39m : \u001b[31mdouble\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstruct\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |    |--\u001b[39m \u001b[31mmood2\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[31m|    |    |    |    |--\u001b[39m \u001b[31melement\u001b[39m : \u001b[31mdouble\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstruct\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |    |    |--\u001b[39m \u001b[31mmood2\u001b[39m : \u001b[31marray\u001b[39m \u001b[31m(nullable = true)\u001b[39m
          |\u001b[31m|    |    |    |    |    |--\u001b[39m \u001b[31melement\u001b[39m : \u001b[31mdouble\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |\u001b[31m|    |    |    |    |--\u001b[39m \u001b[31msomething2\u001b[39m : \u001b[31mstring\u001b[39m \u001b[31m(nullable = false)\u001b[39m
          |                                                                      \u001b[32m|    |--\u001b[39m \u001b[32msomething3\u001b[39m : \u001b[32mstruct\u001b[39m \u001b[32m(nullable = false)\u001b[39m
          |                                                                      \u001b[32m|    |    |--\u001b[39m \u001b[32mmood3\u001b[39m : \u001b[32marray\u001b[39m \u001b[32m(nullable = true)\u001b[39m
          |                                                                      \u001b[32m|    |    |    |--\u001b[39m \u001b[32melement\u001b[39m : \u001b[32mstring\u001b[39m \u001b[32m(nullable = false)\u001b[39m
          |""".stripMargin.replaceAll("\r\n", "\n")

      assert(e.getMessage == expectedMessage)
    }
  }
}
