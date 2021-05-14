package com.github.mrpowers.spark.fast.tests

import java.sql.Date
import java.time.LocalDate
import org.scalatest.FreeSpec

class ArrayUtilTest extends FreeSpec {

  "blah" in {
    val arr: Array[(Any, Any)] = Array(("hi", "there"), ("fun", "train"))
    val res                    = ArrayUtil.weirdTypesToStrings(arr, 10)
    assert(res sameElements Array(List("hi", "there"), List("fun", "train")))
  }

  "showTwoColumnString" in {
    val arr: Array[(Any, Any)] = Array(("word1", "word2"), ("hi", "there"), ("fun", "train"))
    println(ArrayUtil.showTwoColumnString(arr, 10))
  }

  "showTwoColumnDate" in {
    val now = LocalDate.now()
    val arr: Array[(Any, Any)] =
      Array(("word1", "word2"), (Date.valueOf(now), Date.valueOf(now)), (Date.valueOf(now.plusDays(-1)), Date.valueOf(now)))
    println(ArrayUtil.showTwoColumnString(arr, 10))
  }

  "dumbshowTwoColumnString" in {
    val arr: Array[(Any, Any)] = Array(("word1", "word2"), ("hi", "there"), ("fun", "train"))
    val rowEqual               = Array(true, false)
    println(ArrayUtil.showTwoColumnStringColorCustomizable(arr, rowEqual))
  }

}
