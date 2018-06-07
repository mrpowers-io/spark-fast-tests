# spark-fast-tests

A fast Apache Spark testing framework with beautifully formatted error messages!

[![Build Status](https://travis-ci.org/MrPowers/spark-fast-tests.svg?branch=master)](https://travis-ci.org/MrPowers/spark-fast-tests)

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

assertSmallDatasetEquality(sourceDF, expectedDF)
// throws a DatasetSchemaMismatch exception
```

The `assertSmallDatasetEquality` method can also be used to compare Datasets.

```scala
val sourceDS = Seq(
  Person("bob", 1),
  Person("alice", 5)
).toDS

val expectedDS = Seq(
  Person("frank", 10),
  Person("lucy", 5)
).toDS

assertLargeDatasetEquality(sourceDS, expectedDS)
// throws an exception because the Datasets have different data
```

The `DatasetComparer` has `assertSmallDatasetEquality` and `assertLargeDatasetEquality` methods to compare either Datasets or DataFrames.

If you only need to compare DataFrames, you can use `DataFrameComparer` with the associated `assertSmallDataFrameEquality` and `assertLargeDataFrameEquality` methods.  Under the hood, `DataFrameComparer` uses the `assertSmallDatasetEquality` and `assertLargeDatasetEquality`.

## Setup

**Option 1: JitPack**

Update your `build.sbt` file as follows.

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v2.3.0_0.11.0" % "test"
```

**Option 2: Maven**

Fetch the JAR file from Maven.

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.3.0_0.11.0"
```

**NOT RECOMMENDED - Option 3: Spark Packages**

Spark Packages does not let you specify `spark-fast-tests` as a test dependency and is not recommended!

Add the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package) so you can install Spark Packages.

Then add these lines to your `build.sbt` file to install Spark SQL and spark-fast-tests:

```scala
spDependencies += "MrPowers/spark-fast-tests:2.3.0_0.11.0"
sparkComponents ++= Seq("sql")
```

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
object ColumnComparerTest
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

* Don't write code with UDFs
* Test custom transformations and DataFrame functions
* Don't write tests that read from files or write files

## Alternatives

The [spark-testing-base](https://github.com/holdenk/spark-testing-base) project has more features (e.g. streaming support) and is compiled to support a variety of Scala and Spark versions.

You might want to use spark-fast-tests instead of spark-testing-base in these cases:

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

## Contributing

Open an issue or send a pull request to contribute.  Anyone that makes good contributions to the project will be promoted to project maintainer status.

