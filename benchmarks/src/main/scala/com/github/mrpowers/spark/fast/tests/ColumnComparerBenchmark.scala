package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.util.Try

private class ColumnComparerBenchmark extends ColumnComparer {
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime, Mode.SingleShotTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def assertColumnEqualityBenchmarks(blackHole: Blackhole): Boolean = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val ds1 = Seq(
      ("1", "2"),
      ("1", "2"),
      ("1", "2"),
      ("1", "2"),
      ("1", "2"),
      ("1", "2"),
      ("1", "2"),
      ("2", "3")
    ).toDF("col_B", "col_A")

    val result = Try(assertColumnEquality(ds1, "col_B", "col_A"))

    blackHole.consume(result)
    result.isSuccess
  }
}
