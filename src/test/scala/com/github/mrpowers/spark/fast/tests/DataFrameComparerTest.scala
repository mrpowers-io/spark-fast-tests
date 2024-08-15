package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.SchemaComparer.DatasetSchemaMismatch
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
      intercept[DatasetContentMismatch] {
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
      intercept[DatasetCountMismatch] {
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
        ("1", "10/01/2019", 26.762499999999997, Seq(26.762499999999997, 26.762499999999997)),
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
      intercept[DatasetContentMismatch] {
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
      intercept[DatasetContentMismatch] {
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
        ("1", "10/01/2019", 26.762499999999997, Seq(26.762499999999997, 26.762499999999997)),
      ).toDF("col_B", "col_C", "col_A", "col_D")

      assertApproximateSmallDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false)
    }
  }
}
