# spark-fast-tests

A fast Apache Spark testing framework!

The project currently contains traits to support DataFrame and RDD tests.  It will be extended in the future to support streaming and machine learning tests.

For example, it provides an `assertDataFrameEquality` method to compare two DataFrames.

```scala
val sourceDF = Seq(
  (1),
  (5)
).toDF("number")

val expectedDF = Seq(
  (1, "word"),
  (5, "word")
).toDF("number", "word")

assertDataFrameEquality(sourceDF, expectedDF)
// throws a DataFrameSchemaMismatch exception
```

## Setup

Add the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package).

Add these lines to your `build.sbt` file:

```scala
spDependencies += "MrPowers/spark-fast-tests:0.2.0"
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

The `DataFrameComparer` trait defines the `assertDataFrameEquality` method.  Extend your spec file with the `SparkSessionTestWrapper` trait to create DataFrames and the `DataFrameComparer` trait to make DataFrame comparisons.

```scala
class DatasetSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("#alias") {

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

      assertDataFrameEquality(actualDF, expectedDF)

    }

  }

}
```

## Spark Versions

spark-fast-tests supports Spark 2.x.  There are no plans to retrofit the project to work with Spark 1.x.

## Alternatives

The [spark-testing-base](https://github.com/holdenk/spark-testing-base) project more features (e.g. streaming apps) and is compiled to support a variety of Scala and Spark versions.  spark-testing-base is a very good option for many use cases.

You might want to use spark-fast-tests instead when:

* You want to run tests in parallel (you need to set `parallelExecution in Test := false` with spark-testing-base)
* You don't want to include hive as a project dependency
* You want a test suite that runs faster because you don't restart the SparkSession after each test file runs

## Additional Goals

* Spark test runs consume a lot of memory and we want to consume memory sparingly
* Provide readable error messages
* Easy to use in conjunction with other test suites

