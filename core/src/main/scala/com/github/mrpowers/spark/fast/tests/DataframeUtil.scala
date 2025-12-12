package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
import com.github.mrpowers.spark.fast.tests.ufansi.{EscapeAttr, Str}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object DataframeUtil {
  private[mrpowers] def showDataframeDiff(
      actual: Array[Row],
      expected: Array[Row],
      truncate: Int = 20,
      minColWidth: Int = 3
  ): String = {

    val sb          = new StringBuilder
    val fields      = actual.head.schema.fieldNames
    val rowWidths   = getColWidths(fields, actual.toSeq ++ expected.toSeq)
    val indexes     = actual.indices.map(v => v + 1).map(_.toString + ":")
    val indColWidth = indexes.map(_.length).max + 1

    val fullJoinWithEquals = actual
      .zipAll(expected, Row(), Row())
      .map { case (actualRow, expectedRow) => (actualRow, expectedRow, actualRow.equals(expectedRow)) }
    val diff = fullJoinWithEquals.map { case (actualRow, expectedRow, rowsAreEqual) =>
      val paddedActualRow   = pad(actualRow, truncate, rowWidths)
      val paddedExpectedRow = pad(expectedRow, truncate, rowWidths)
      if (rowsAreEqual) {
        List(DarkGray(paddedActualRow.mkString("|")), DarkGray(paddedActualRow.mkString("|")))
      } else {
        val actualSeq   = actualRow.toSeq
        val expectedSeq = expectedRow.toSeq
        if (actualSeq.isEmpty)
          List(
            Red(" " * rowWidths.sum),
            Green(paddedExpectedRow.mkString("", "|", ""))
          )
        else if (expectedSeq.isEmpty)
          List(Red(paddedActualRow.mkString("", "|", "")), Green("" * rowWidths.sum))
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
                  val paddedActualRow = padAny(actualRowField, truncate, rowWidths(i))
                  val paddedExpected  = padAny(expectedRowField, truncate, rowWidths(i))
                  (DarkGray(paddedActualRow), DarkGray(paddedExpected))
                case ((actualRowField, expectedRowField, false), i) =>
                  val paddedActualRow = padAny(actualRowField, truncate, rowWidths(i))
                  val paddedExpected  = padAny(expectedRowField, truncate, rowWidths(i))
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

    val leftPaddedFields      = pad(fields, truncate, rowWidths)
    val fieldsHeader          = List(leftPaddedFields.mkString("|"))
    val colWidths: Array[Int] = getColWidths(minColWidth, diff)

    // Create SeparateLine
    val sep: String =
      rowWidths
        .map("-" * _)
        .mkString(StringUtils.leftPad("+", indColWidth), "+", "+\n")

    sb.append(sep)

    fieldsHeader.zipWithIndex
      .map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i))
        } else {
          StringUtils.rightPad(cell, colWidths(i))
        }
      }
      .addString(sb, StringUtils.leftPad("|", indColWidth), "|", "|\n")
    pad(diff, truncate, colWidths).zipWithIndex.map { case (actual :: expected :: Nil, i) =>
      val indexString = StringUtils.leftPad(s"${i + 1}:|", indColWidth)
      sb.append(indexString)
      sb.append(actual)
      sb.append(s"|:${i + 1}\n")
      val rowsAreDifferent = !fullJoinWithEquals(i)._3
      if (rowsAreDifferent) {
        sb.append(indexString)
        sb.append(expected)
        sb.append(s"|:${i + 1}\n")
        if (i < diff.length - 1)
          sb.append("\n")
      } else if (i < diff.length - 1 && !fullJoinWithEquals(i + 1)._3) {
        // if current rows are equal and next ones are not
        sb.append("\n")
      }
    }
    sb.append(sep).toString()
  }

  private def pad(row: Row, truncate: Int, colWidths: Array[Int]): Seq[String] =
    pad(
      row.toSeq.map { v =>
        Option(v).map(_.toString).getOrElse("null")
      },
      truncate,
      colWidths
    )

  private def pad(
      rows: Array[List[Str]],
      truncate: Int,
      colWidths: Array[Int],
      spacesColor: EscapeAttr = DarkGray
  ) =
    rows.map { row =>
      row.zipWithIndex
        .map { case (cell, i) =>
          val padsLen = colWidths(i) - cell.length
          val pads    = if (padsLen > 0) spacesColor(" " * padsLen) else spacesColor("")
          if (truncate > 0) {
            pads ++ cell
          } else {
            cell ++ pads
          }

        }
    }

  private def padAny(s: Any, truncate: Int, width: Int) = {
    val cell = Option(s).map(_.toString).getOrElse("null")
    if (truncate > 0) {
      StringUtils.leftPad(cell, width)
    } else {
      StringUtils.rightPad(cell, width)
    }
  }

  private def pad(header: Seq[String], truncate: Int, colWidths: Array[Int]) =
    header.zipWithIndex
      .map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i))
        } else {
          StringUtils.rightPad(cell, colWidths(i))
        }
      }

  private def getColWidths(minColWidth: Int, rows: Array[List[Str]]) = {
    val numCols = rows.map(_.length).max
    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minColWidth)

    for (row <- rows)
      for ((cell, i) <- row.zipWithIndex)
        colWidths(i) = math.max(colWidths(i), cell.length)
    colWidths
  }

  private def getColWidths(fields: Array[String], rows: Seq[Row]) = {
    val numCols = rows.map(_.size).max
    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(0)
    for ((cell, i) <- fields.zipWithIndex)
      colWidths(i) = math.max(colWidths(i), cell.length)
    // Compute the width of each column
    for (row <- rows)
      for ((cell, i) <- row.toSeq.zipWithIndex)
        if (cell != null)
          colWidths(i) = math.max(colWidths(i), cell.toString.length)
    colWidths
  }

}
