package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.DarkGray
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
import org.scalameter.api._

object MkStrBenchmark extends Bench.ForkedTime {
  val ranges = for {
    size <- Gen.range("size")(100, 1000, 100)
  } yield 0 until size
  val strings = ranges.map(_.toList.map(v => DarkGray("a".repeat(100))))
  val start   = DarkGray("[")
  val sep     = DarkGray(",")
  val end     = DarkGray("]")
  measure method "mkStringReduce" in {
    using(strings) curve ("Str") in {
      _.mkStrReduce(start, sep, end)
    }
  }
  measure method "mkStringFoldLeft" in {
    using(strings) curve ("Str") in {
      _.mkStrFoldLeft(start, sep, end)
    }
  }
}
