# spark-fast-tests

A fast, test framework independent Apache Spark testing helper library with beautifully formatted error messages!

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ab42211c18984740bee7f87c631a8f42)](https://www.codacy.com/app/MrPowers/spark-fast-tests?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MrPowers/spark-fast-tests&amp;utm_campaign=Badge_Grade)

For example, the `assertSmallDatasetEquality` method can be used to compare two Datasets (or two DataFrames).

```scala
val sourceDF = Seq(
  (1),
  (5)
).toDF("number")

val expectedDF = Seq(
  (1, "word"),
  (5, "word")
).toDF("number", "word")

assertSmallDataFrameEquality(sourceDF, expectedDF)
// throws a DatasetSchemaMismatch exception
```

The `assertSmallDatasetEquality` method can also be used to compare Datasets.

```scala
val sourceDS = Seq(
  Person("juan", 5),
  Person("bob", 1),
  Person("li", 49),
  Person("alice", 5)
).toDS

val expectedDS = Seq(
  Person("juan", 5),
  Person("frank", 10),
  Person("li", 49),
  Person("lucy", 5)
).toDS
```

![assert_small_dataset_equality_error_message](https://github.com/MrPowers/spark-fast-tests/blob/master/images/assertSmallDatasetEquality_error_message.png)

The colors in the error message make it easy to identify the rows that aren't equal.

The `DatasetComparer` has `assertSmallDatasetEquality` and `assertLargeDatasetEquality` methods to compare either Datasets or DataFrames.

If you only need to compare DataFrames, you can use `DataFrameComparer` with the associated `assertSmallDataFrameEquality` and `assertLargeDataFrameEquality` methods.  Under the hood, `DataFrameComparer` uses the `assertSmallDatasetEquality` and `assertLargeDatasetEquality`.

## Setup

**Option 1: Maven**

Fetch the JAR file from Maven.

```scala
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests_2.11" % "0.12.0" % "test"
```

**Option 2: JitPack**

Update your `build.sbt` file as follows.

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v2.3.0_0.12.0" % "test"
```

## Why is this library fast?

The `assertSmallDataFrameEquality` method runs 31% faster than the `assertLargeDatasetEquality` method as described in [this blog post](https://medium.com/@mrpowers/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f).

The `assertSmallDataFrameEquality` method uses the Dataset `collect()` method, which is a lot faster than the RDD `zipWithIndex()` method that's used by the other Spark testing libraries (and the `assertLargeDatasetEquality()` method).

spark-fast-tests also provides a `assertColumnEquality()` method that's even faster and easier to use!

## Usage

The spark-fast-tests project doesn't provide a SparkSession object in your test suite, so you'll need to make one yourself.

```scala
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
```

The `DatasetComparer` trait defines the `assertSmallDatasetEquality` method.  Extend your spec file with the `SparkSessionTestWrapper` trait to create DataFrames and the `DatasetComparer` trait to make DataFrame comparisons.

```scala
import com.github.mrpowers.spark.fast.tests.DatasetComparer

class DatasetSpec extends FunSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

    it("aliases a DataFrame") {

      val sourceDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDF = sourceDF.select(col("name").alias("student"))

      val expectedDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

}
```

To compare large DataFrames that are partitioned across different nodes in a cluster, use the `assertLargeDatasetEquality` method.

```scala
assertLargeDatasetEquality(actualDF, expectedDF)
```

`assertSmallDatasetEquality` is faster for test suites that run on your local machine.  `assertLargeDatasetEquality` should only be used for DataFrames that are split across nodes in a cluster.

### Column Equality

The `assertColumnEquality` method can be used to assess the equality of two columns in a DataFrame.

Suppose you have the following DataFrame with two columns that are not equal.

```
+-------+-------------+
|   name|expected_name|
+-------+-------------+
|   phil|         phil|
| rashid|       rashid|
|matthew|        mateo|
|   sami|         sami|
|     li|         feng|
|   null|         null|
+-------+-------------+
```

The following code will throw a `ColumnMismatch` error message:

```scala
assertColumnEquality(df, "name", "expected_name")
```

![assert_column_equality_error_message](https://github.com/MrPowers/spark-fast-tests/blob/master/images/assertColumnEquality_error_message.png)

Mix in the `ColumnComparer` trait to your test class to access the `assertColumnEquality` method:

```scala
import com.github.mrpowers.spark.fast.tests.ColumnComparer

object MySpecialClassTest
    extends TestSuite
    with ColumnComparer
    with SparkSessionTestWrapper {

    // your tests
}
```

### Unordered DataFrame equality comparisons

Suppose you have the following `actualDF`:

```
+------+
|number|
+------+
|     1|
|     5|
+------+
```

And suppose you have the following `expectedDF`:

```
+------+
|number|
+------+
|     5|
|     1|
+------+
```

The DataFrames have the same columns and rows, but the order is different.

`assertSmallDataFrameEquality(sourceDF, expectedDF)` will throw a `DatasetContentMismatch` error.

We can set the `orderedComparison` boolean flag to `false` and spark-fast-tests will sort the DataFrames before performing the comparison.

`assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)` will not throw an error.

### Equality comparisons ignoring the nullable flag

You might also want to make equality comparisons that ignore the nullable flags for the DataFrame columns.

Here is how to use the `ignoreNullable` flag to compare DataFrames without considering the nullable property of each column.

```scala
val sourceDF = spark.createDF(
  List(
    (1),
    (5)
  ), List(
    ("number", IntegerType, false)
  )
)

val expectedDF = spark.createDF(
  List(
    (1),
    (5)
  ), List(
    ("number", IntegerType, true)
  )
)

assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
```

## Testing Tips

* Use column functions instead of UDFs as described in [this blog post](https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b)
* Try to organize your code as [custom transformations](https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c) so it's easy to test the logic elegantly
* Don't write tests that read from files or write files.  Dependency injection is a great way to avoid file I/O in you test suite.

## uTest settings to display color output

Create a `CustomFramework` class with overrides that turn off the default uTest color settings.

```scala
package com.github.mrpowers.spark.fast.tests

class CustomFramework extends utest.runner.Framework {
  override def formatWrapWidth: Int = 300
  // turn off the default exception message color, so spark-fast-tests
  // can send messages with custom colors
  override def exceptionMsgColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionPrefixColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionMethodColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionPunctuationColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionLineNumberColor = toggledColor(utest.ufansi.Attrs.Empty)
}
```

Update the `build.sbt` file to use the `CustomFramework` class:

```scala
testFrameworks += new TestFramework("com.github.mrpowers.spark.fast.tests.CustomFramework")
```

## Alternatives

The [spark-testing-base](https://github.com/holdenk/spark-testing-base) project has more features (e.g. streaming support) and is compiled to support a variety of Scala and Spark versions.

You might want to use spark-fast-tests instead of spark-testing-base in these cases:

* You want to use uTest or a testing framework other than scalatest
* You want to run tests in parallel (you need to set `parallelExecution in Test := false` with spark-testing-base)
* You don't want to include hive as a project dependency
* You don't want to restart the SparkSession after each test file executes so the suite runs faster

## Additional Goals

* Use memory efficiently so Spark test runs don't crash
* Provide readable error messages
* Easy to use in conjunction with other test suites
* Give the user control of the SparkSession

## Spark Versions

spark-fast-tests supports Spark 2.x.  There are no plans to retrofit the project to work with Spark 1.x.

## Publishing

Only project maintainers can publish JAR files.

For JitPack, run the `scripts/multi_spark_releases.sh` script to make a bunch of releases in GitHub that'll be picked up by JitPack.  You need to install [hub](https://github.com/github/hub) and pass in the [spark-daria](https://github.com/MrPowers/spark-daria) `github_release.sh` script as an argument to successfully run this command.

For Maven, follow [this guide](https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/) to setup your computer and then run these commands.

```
sbt publishSigned
sbt sonatypeRelease
```

## Contributing

Open an issue or send a pull request to contribute.  Anyone that makes good contributions to the project will be promoted to project maintainer status.

