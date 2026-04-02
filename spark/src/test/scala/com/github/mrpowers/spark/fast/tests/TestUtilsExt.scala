package com.github.mrpowers.spark.fast.tests

import scala.util.matching.Regex

object TestUtilsExt {
  val coloredStringPattern: Regex = "(\u001B\\[\\d{1,2}m)([\\s\\S]*?)(?=\u001B\\[\\d{1,2}m)".r
  implicit class StringOps(s: String) {
    def extractColorGroup: Map[String, Seq[String]] = coloredStringPattern
      .findAllMatchIn(s)
      .map(m => (m.group(1), m.group(2)))
      .toSeq
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .view
      .toMap

    def assertColorDiff(actual: Seq[String], expected: Seq[String]): Unit = {
      val colourGroup         = extractColorGroup
      val expectedColourGroup = colourGroup.get(Console.GREEN)
      val actualColourGroup   = colourGroup.get(Console.RED)
      assert(expectedColourGroup.contains(expected))
      assert(actualColourGroup.contains(actual))
    }
  }

  implicit class ExceptionOps(e: Exception) {
    def assertColorDiff(actual: Seq[String], expected: Seq[String]): Unit = e.getMessage.assertColorDiff(actual, expected)
  }
}
