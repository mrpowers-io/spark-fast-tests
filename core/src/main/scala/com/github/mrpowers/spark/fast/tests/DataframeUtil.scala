package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
object DataframeUtil {

  private[mrpowers] def showDataframeDiff(
      header: (String, String),
      actual: Seq[Row],
      expected: Seq[Row],
      truncate: Int = 20,
      minColWidth: Int = 3
  ): String = {

    val sb = new StringBuilder

    val fullJoin = actual.zipAll(expected, Row(), Row())
    val diff = fullJoin.map { case (actualRow, expectedRow) =>
      if (equals(actualRow, expectedRow)) {
        List(DarkGray(actualRow.toString), DarkGray(expectedRow.toString))
      } else {
        val actualSeq   = actualRow.toSeq
        val expectedSeq = expectedRow.toSeq
        if (actualSeq.isEmpty)
          List(
            Red("[]"),
            Green(expectedSeq.mkString("[", ",", "]"))
          )
        else if (expectedSeq.isEmpty)
          List(Red(actualSeq.mkString("[", ",", "]")), Green("[]"))
        else {
          val withEquals = actualSeq
            .zip(expectedSeq)
            .map { case (actualRowField, expectedRowField) =>
              (actualRowField, expectedRowField, actualRowField == expectedRowField)
            }
          val allFieldsAreNotEqual = !withEquals.exists(_._3)
          if (allFieldsAreNotEqual) {
            List(
              Red(actualSeq.mkString("[", ",", "]")),
              Green(expectedSeq.mkString("[", ",", "]"))
            )
          } else {

            val coloredDiff = withEquals
              .map {
                case (actualRowField, expectedRowField, true) =>
                  (DarkGray(actualRowField.toString), DarkGray(expectedRowField.toString))
                case (actualRowField, expectedRowField, false) =>
                  (Red(actualRowField.toString), Green(expectedRowField.toString))
              }
            val start = DarkGray("[")
            val sep   = DarkGray(",")
            val end   = DarkGray("]")
            List(
              coloredDiff.map(_._1).mkStr(start, sep, end),
              coloredDiff.map(_._2).mkStr(start, sep, end)
            )
          }
        }
      }
    }
    val headerSeq = List(header._1, header._2)
    val numCols   = 2

    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minColWidth)

    // Compute the width of each column
    for ((cell, i) <- headerSeq.zipWithIndex) {
      colWidths(i) = math.max(colWidths(i), cell.length)
    }
    for (row <- diff) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String =
      colWidths
        .map("-" * _)
        .addString(sb, "+", "+", "+\n")
        .toString

    // column names
    headerSeq.zipWithIndex
      .map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i))
        } else {
          StringUtils.rightPad(cell, colWidths(i))
        }
      }
      .addString(sb, "|", "|", "|\n")

    sb.append(sep)

    diff.map { row =>
      row.zipWithIndex
        .map { case (cell, i) =>
          val padsLen = colWidths(i) - cell.length
          val pads    = if (padsLen > 0) " " * padsLen else ""
          if (truncate > 0) {
            pads + cell.toString
          } else {
            cell.toString + pads
          }

        }
        .addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    sb.toString
  }

}
