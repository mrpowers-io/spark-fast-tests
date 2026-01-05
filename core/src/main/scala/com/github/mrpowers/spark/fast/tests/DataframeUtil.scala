package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
import com.github.mrpowers.spark.fast.tests.ufansi.Str
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object DataframeUtil {
  private val EmptyStr = Str("")
  private[mrpowers] def showDataframeDiff(
      actual: Array[Row],
      expected: Array[Row],
      fieldNames: Array[String],
      truncate: Int = 20,
      minColWidth: Int = 3
  ): String = {
    val sb        = new StringBuilder
    val colWidths = getColWidths(fieldNames, actual.toSeq ++ expected.toSeq, truncate, minColWidth)
    // needed to calculate padding for other elements
    val largestIndexOffset = {
      if (actual.isEmpty && expected.isEmpty) 0
      else {
        val maxIndex     = Math.max(actual.length, expected.length) - 1
        val largestIndex = maxIndex.toString + ":" // Largest index that is visible on the side. Like `100:`
        largestIndex.length + 1
      }
    }

    val fullJoinWithEquals = actual
      .zipAll(expected, Row.empty, Row.empty)
      .map { case (actualRow, expectedRow) => (actualRow, expectedRow, actualRow.equals(expectedRow)) }
    val diff = fullJoinWithEquals.map { case (actualRow, expectedRow, rowsAreEqual) =>
      lazy val paddedActualRow   = pad(actualRow.toSeq, colWidths, truncate)
      lazy val paddedExpectedRow = pad(expectedRow.toSeq, colWidths, truncate)
      if (rowsAreEqual) {
        List(DarkGray(paddedActualRow.mkString("|")), DarkGray(paddedActualRow.mkString("|")))
      } else {
        val actualSeq   = actualRow.toSeq
        val expectedSeq = expectedRow.toSeq
        if (actualSeq.isEmpty)
          List(
            EmptyStr,
            Green(paddedExpectedRow.mkString("", "|", ""))
          )
        else if (expectedSeq.isEmpty)
          List(Red(paddedActualRow.mkString("", "|", "")), EmptyStr)
        else {
          val withEquals = actualSeq
            .zip(expectedSeq)
            .map { case (actualRowField, expectedRowField) =>
              (actualRowField, expectedRowField, actualRowField == expectedRowField)
            }
          val allFieldsAreNotEqual = !withEquals.exists(_._3)
          if (allFieldsAreNotEqual) {
            List(
              Red(paddedActualRow.mkString("", "|", "")),
              Green(paddedExpectedRow.mkString("", "|", ""))
            )
          } else {
            val coloredDiff = withEquals.zipWithIndex
              .map {
                case ((actualRowField, expectedRowField, true), i) =>
                  val paddedActualRow = padAny(actualRowField, colWidths(i), truncate)
                  val paddedExpected  = padAny(expectedRowField, colWidths(i), truncate)
                  (DarkGray(paddedActualRow), DarkGray(paddedExpected))
                case ((actualRowField, expectedRowField, false), i) =>
                  val paddedActualRow = padAny(actualRowField, colWidths(i), truncate)
                  val paddedExpected  = padAny(expectedRowField, colWidths(i), truncate)
                  (Red(paddedActualRow), Green(paddedExpected))
              }
            val start = DarkGray("")
            val sep   = DarkGray("|")
            val end   = DarkGray("")
            List(
              coloredDiff.map(_._1).mkStr(start, sep, end),
              coloredDiff.map(_._2).mkStr(start, sep, end)
            )
          }
        }
      }
    }

    val headerWithLeftPadding = pad(fieldNames, colWidths, truncate)
    val headerFields          = List(headerWithLeftPadding.mkString("|"))

    val separatorLine: String =
      colWidths
        .map("-" * _)
        .mkString(StringUtils.leftPad("+", largestIndexOffset), "+", "+\n")

    sb.append(separatorLine)

    headerFields
      .zip(colWidths)
      .map { case (cell, colWidth) =>
        StringUtils.leftPad(cell, colWidth)
      }
      .addString(sb, StringUtils.leftPad("|", largestIndexOffset), "|", "|\n")
    diff.zipWithIndex.foreach { case (actual :: expected :: Nil, i) =>
      def appendRow(row: Str, i: Int): Unit = {
        if (row.length > 0) {
          val indexString = StringUtils.leftPad(s"${i + 1}:|", largestIndexOffset)
          sb.append(indexString)
          sb.append(row)
          sb.append(s"|:${i + 1}\n")
        }
      }
      appendRow(actual, i)
      val rowsAreDifferent = !fullJoinWithEquals(i)._3
      if (rowsAreDifferent) {
        appendRow(expected, i)
        if (i < diff.length - 1)
          sb.append(separatorLine)
      } else if (i < diff.length - 1 && !fullJoinWithEquals(i + 1)._3) { // if current rows are equal and next ones are not
        sb.append(separatorLine)
      }
    }
    sb.append(separatorLine).toString()
  }

  private def pad(items: Seq[Any], colWidths: Array[Int], truncateColumnLen: Int): Seq[String] =
    items.zip(colWidths).map { case (v, colWidth) => padAny(v, colWidth, truncateColumnLen) }

  private def padAny(s: Any, width: Int, truncateColumnLen: Int) = {
    StringUtils.leftPad(DataFramePrettyPrint.cellToString(s, truncateColumnLen), width)
  }

  private def getColWidths(fields: Array[String], rows: Seq[Row], truncate: Int, minColWidth: Int) = {
    val numCols = if (rows.isEmpty) 0 else rows.map(_.size).max
    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minColWidth)
    for ((cell, i) <- fields.zipWithIndex) {
      colWidths(i) = math.max(colWidths(i), DataFramePrettyPrint.cellToString(cell, truncate).length)
    }
    // Compute the width of each column
    for (row <- rows)
      for ((cell, i) <- row.toSeq.zipWithIndex)
        colWidths(i) = math.max(colWidths(i), DataFramePrettyPrint.cellToString(cell, truncate).length)
    colWidths
  }

}
