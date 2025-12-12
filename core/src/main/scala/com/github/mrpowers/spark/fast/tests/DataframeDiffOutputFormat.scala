package com.github.mrpowers.spark.fast.tests

object DataframeDiffOutputFormat extends Enumeration {
  type DataframeDiffOutputFormat = Value
  val SideBySide, SeparateLines = Value
}
