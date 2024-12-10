package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.util.Try

private class DatasetComparerBenchmark extends DatasetComparer {
  def getSparkSession: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertLargeDatasetEqualityV2(blackHole: Blackhole): Boolean = {
    val spark = getSparkSession
    val ds1   = spark.range(0, 1000000, 1, 8)
    val ds3   = ds1

    val result = Try(assertLargeDatasetEqualityV2(ds1, ds3))

    blackHole.consume(result)
    result.isSuccess
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertLargeDatasetEqualityV2WithSinglePrimaryKey(blackHole: Blackhole): Boolean = {
    val spark = getSparkSession
    val ds1   = spark.range(0, 1000000, 1, 8)
    val ds3   = ds1

    val result = Try(assertLargeDatasetEqualityV2(ds1, ds3, primaryKeys = Seq("id")))

    blackHole.consume(result)
    result.isSuccess
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertLargeDatasetEquality(blackHole: Blackhole): Boolean = {
    val spark = getSparkSession
    val ds1   = spark.range(0, 1000000, 1, 8)
    val ds3   = ds1

    val result = Try(assertLargeDatasetEquality(ds1, ds3))

    blackHole.consume(result)
    result.isSuccess
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertLargeDatasetEqualityV2WithCompositePrimaryKey2(blackHole: Blackhole): Boolean = {
    val spark  = getSparkSession
    val ds1    = spark.range(0, 1000000, 1, 8).withColumn("id2", col("id") + 1)
    val ds3    = ds1
    val result = Try(assertLargeDatasetEqualityV2(ds1, ds3, primaryKeys = Seq("id", "id2")))

    blackHole.consume(result)
    result.isSuccess
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertLargeDatasetEqualityV2WithCompositePrimaryKey3(blackHole: Blackhole): Boolean = {
    val spark  = getSparkSession
    val ds1    = spark.range(0, 1000000, 1, 8).withColumn("id2", col("id") + 1).withColumn("id3", col("id2") + 1)
    val ds3    = ds1
    val result = Try(assertLargeDatasetEqualityV2(ds1, ds3, primaryKeys = Seq("id", "id2", "id3")))

    blackHole.consume(result)
    result.isSuccess
  }
}
