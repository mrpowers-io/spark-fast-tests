package com.github.mrpowers.spark.fast.tests

object DataframeDiffOutputFormat extends Enumeration {
  type DisplayFormat = Value
  val SideBySide, SeparateLines = Value
}