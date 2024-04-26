package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.time.Instant
import java.util.concurrent.TimeUnit

// TODO: move this to separate benchmark project
private class MyBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @Fork(value = 2)
  @Warmup(iterations = 10)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  def testMethod(blackHole: Blackhole): Boolean = {
    val r1 = Row(
      "a",
      Row(
        1,
        Row(
          2.0,
          Row(
            null,
            Row(
              Seq(Row("c"), Row("d")),
              BigDecimal.decimal(1.0),
              Row(Instant.EPOCH)
            )
          )
        )
      )
    )

    val r2 = Row(
      "a",
      Row(
        1,
        Row(
          2.0,
          Row(
            null,
            Row(
              Seq(Row("c"), Row("d")),
              BigDecimal.decimal(1.0),
              Row(Instant.EPOCH)
            )
          )
        )
      )
    )
    val bool = RowComparer.areRowsEqual(r1, r2)
    blackHole.consume(bool)
    bool
  }
}
