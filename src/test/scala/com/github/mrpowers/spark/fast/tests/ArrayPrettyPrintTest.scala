package com.github.mrpowers.spark.fast.tests

import org.scalatest.FreeSpec

class ArrayPrettyPrintTest extends FreeSpec {

  "blah" in {
    val arr: Array[(Any, Any)] = Array(("hi", "there"), ("fun", "train"))
    val res                    = ArrayPrettyPrint.weirdTypesToStrings(arr, 10)
    assert(res sameElements Array(List("hi", "there"), List("fun", "train")))
  }

  "showTwoColumnString" in {
    val arr: Array[(Any, Any)] = Array(("word1", "word2"), ("hi", "there"), ("fun", "train"))
    println(ArrayPrettyPrint.showTwoColumnString(arr, 10))
  }

  "dumbshowTwoColumnString" in {
    val arr: Array[(Any, Any)] = Array(("word1", "word2"), ("hi", "there"), ("fun", "train"))
    val rowEqual               = Array(true, false)
    println(ArrayPrettyPrint.showTwoColumnStringColorCustomizable(arr, rowEqual))
  }

}
