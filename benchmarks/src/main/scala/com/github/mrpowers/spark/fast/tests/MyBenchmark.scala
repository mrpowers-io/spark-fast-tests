package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.util.Try

private class MyBenchmark extends DataFrameComparer {
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime, Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertApproximateDataFrameEqualityWithPrecision(blackHole: Blackhole): Boolean = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val ds1 = Seq(
      ("1", "10/01/2019", 26.762499999999996),
      ("1", "11/01/2019", 26.762499999999996)
    ).toDF("col_B", "col_C", "col_A")

    val ds2 = Seq(
      ("1", "10/01/2019", 26.762499999999946),
      ("1", "11/01/2019", 26.76249999999991)
    ).toDF("col_B", "col_C", "col_A")
    val result = Try(assertApproximateDataFrameEquality(ds1, ds2, precision = 0.0000001, orderedComparison = false))

    blackHole.consume(result)
    result.isSuccess
  }
}
