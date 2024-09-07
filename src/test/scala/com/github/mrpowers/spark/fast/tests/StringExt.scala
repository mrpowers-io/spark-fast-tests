package com.github.mrpowers.spark.fast.tests

import scala.util.matching.Regex

object StringExt {
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
  }
}
