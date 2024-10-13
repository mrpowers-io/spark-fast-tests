package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{DoubleType, IntegerType, MetadataBuilder, StringType}
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.SchemaComparer.DatasetSchemaMismatch
import com.github.mrpowers.spark.fast.tests.StringExt.StringOps
import org.apache.spark.sql.functions.col
import org.scalatest.freespec.AnyFreeSpec

class DataFrameComparerTest extends AnyFreeSpec with DataFrameComparer with SparkSessionTestWrapper {

  "prints a descriptive error message if it bugs out" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }
    assert(e.getMessage.indexOf("bob") >= 0)
    assert(e.getMessage.indexOf("camila") >= 0)
  }

  "Correctly mark unequal elements" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru"),
        ("steve", 10, "aus")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru"),
        ("mark", 11, "usa")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(expectedDF, sourceDF)
    }

    val colourGroup         = e.getMessage.extractColorGroup
    val expectedColourGroup = colourGroup.get(Console.GREEN)
    val actualColourGroup   = colourGroup.get(Console.RED)
    assert(expectedColourGroup.contains(Seq("uk", "[steve,10,aus]")))
    assert(actualColourGroup.contains(Seq("france", "[mark,11,usa]")))
  }

  "works well for wide DataFrames" in {
    val sourceDF = spark.createDF(
      List(
        ("bobisanicepersonandwelikehimOK", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bobisanicepersonandwelikehimNOT", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }
  }

  "also print a descriptive error message if the right side is missing" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru"),
        ("jean-jacques", 4, "france")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.indexOf("jean-jacques") >= 0)
  }

  "also print a descriptive error message if the left side is missing" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru"),
        ("jean-claude", 4, "france")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.indexOf("jean-claude") >= 0)
  }

  "assertSmallDataFrameEquality" - {

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      assertLargeDataFrameEquality(sourceDF, expectedDF)
    }

    "throws an error if the DataFrames have different schemas" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }
      intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (10),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      intercept[DatasetContentMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }
      intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }
    }

    "can performed unordered DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1)
        ),
        List(("number", IntegerType, true))
      )
      assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "throws an error for unordered DataFrame comparisons that don't match" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1),
          (10)
        ),
        List(("number", IntegerType, true))
      )
      intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
      }
    }

    "can performed DataFrame comparisons with unordered column" in {
      val sourceDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )
      val expectedDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )
      assertLargeDataFrameEquality(sourceDF, expectedDF, ignoreColumnOrder = true)
    }

    "should not ignore nullable if ignoreNullable is false" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )

      intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }
    }
  }

  "assertApproximateDataFrameEquality" - {

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
    }

    "throws an error if the rows are different" in {
      val sourceDF = spark.createDF(
        List(
          100.9,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          8.8
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetCountMismatch] {
        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "should not ignore nullable if ignoreNullable is false" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )

      intercept[DatasetSchemaMismatch] {
        assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "can ignore the nullable property" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreNullable = true)
    }

    "can ignore the column names" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("BLAHBLBH", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreColumnNames = true)
    }

    "can work with precision and unordered comparison" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996),
        ("1", "11/01/2019", 26.762499999999996)
      ).toDF("col_B", "col_C", "col_A")

      val ds2 = Seq(
        ("1", "10/01/2019", 26.762499999999946),
        ("1", "11/01/2019", 26.76249999999991)
      ).toDF("col_B", "col_C", "col_A")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison 2" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996, "A"),
        ("1", "10/01/2019", 26.762499999999996, "B")
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("1", "10/01/2019", 26.762499999999946, "A"),
        ("1", "10/01/2019", 26.76249999999991, "B")
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison on nested column" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996)),
        ("1", "11/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("1", "11/01/2019", 26.7624999999999961, Seq(26.7624999999999961, 26.7624999999999961)),
        ("1", "10/01/2019", 26.762499999999997, Seq(26.762499999999997, 26.762499999999997))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }
  }

  "assertApproximateSmallDataFrameEquality" - {

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01)
    }

    "throws an error if the rows are different" in {
      val sourceDF = spark.createDF(
        List(
          100.9,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          8.8
        ),
        List(("number", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "can ignore the nullable property" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreNullable = true)
    }

    "should not ignore nullable if ignoreNullable is false" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, false))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1
        ),
        List(("number", DoubleType, true))
      )

      intercept[DatasetSchemaMismatch] {
        assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01)
      }
    }

    "can ignore the column names" in {
      val sourceDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("BLAHBLBH", DoubleType, true))
      )
      val expectedDF = spark.createDF(
        List(
          1.2,
          5.1,
          null
        ),
        List(("number", DoubleType, true))
      )
      assertApproximateSmallDataFrameEquality(sourceDF, expectedDF, 0.01, ignoreColumnNames = true)
    }

    "can work with precision and unordered comparison" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996),
        ("1", "11/01/2019", 26.762499999999996)
      ).toDF("col_B", "col_C", "col_A")

      val ds2 = Seq(
        ("1", "10/01/2019", 26.762499999999946),
        ("1", "11/01/2019", 26.76249999999991)
      ).toDF("col_B", "col_C", "col_A")

      assertApproximateSmallDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison 2" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", "A", 26.762499999999996),
        ("1", "10/01/2019", "B", 26.762499999999996)
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("1", "10/01/2019", "A", 26.762499999999946),
        ("1", "10/01/2019", "B", 26.76249999999991)
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateSmallDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }

    "can work with precision and unordered comparison on nested column" in {
      import spark.implicits._
      val ds1 = Seq(
        ("1", "10/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996)),
        ("2", "11/01/2019", 26.762499999999996, Seq(26.762499999999996, 26.762499999999996))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      val ds2 = Seq(
        ("2", "11/01/2019", 26.7624999999999961, Seq(26.7624999999999961, 26.7624999999999961)),
        ("1", "10/01/2019", 26.762499999999997, Seq(26.762499999999997, 26.762499999999997))
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateSmallDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }
  }
}
