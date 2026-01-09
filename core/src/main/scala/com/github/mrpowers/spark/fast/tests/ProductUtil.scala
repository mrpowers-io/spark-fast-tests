package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
import com.github.mrpowers.spark.fast.tests.ufansi.Str
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object ProductUtil {
  private[mrpowers] def productOrRowToSeq(product: Any): Seq[Any] = {
    product match {
      case null           => Seq.empty
      case a: Array[_]    => a
      case i: Iterable[_] => i.toSeq
      case r: Row         => r.toSeq
      case p: Product     => p.productIterator.toSeq
      case s              => Seq(s)
    }
  }

  private def rowFieldToString(fieldValue: Any): String = s"$fieldValue"

  private[mrpowers] def showProductDiff[T: ClassTag](
      columns: Array[String],
      actual: Seq[T],
      expected: Seq[T],
      truncate: Int = 20,
      minColWidth: Int = 3,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): String = {
    outputFormat match {
      case DataframeDiffOutputFormat.SideBySide =>
        showProductDiffSideBySide(Seq("Actual Content", "Expected Content"), actual, expected, truncate, minColWidth)
      case DataframeDiffOutputFormat.SeparateLines =>
        showProductDiffSeparateLine(actual, expected, columns, truncate, minColWidth)
    }
  }

  private[mrpowers] def showProductDiffSideBySide[T: ClassTag](
      header: Seq[String],
      actual: Seq[T],
      expected: Seq[T],
      truncate: Int = 20,
      minColWidth: Int = 3
  ): String = {

    val runTimeClass                     = implicitly[ClassTag[T]].runtimeClass
    val (className, lBracket, rBracket)  = if (runTimeClass == classOf[Row]) ("", "[", "]") else (runTimeClass.getSimpleName, "(", ")")
    val prodToString: Seq[Any] => String = s => s.mkString(s"$className$lBracket", ",", rBracket)
    val emptyProd                        = "MISSING"

    val sb = new StringBuilder

    val diff = actual
      .zipAll(expected, null, null)
      .map { case (actualRow, expectedRow) =>
        if (actualRow == expectedRow) {
          List(DarkGray(actualRow.toString), DarkGray(expectedRow.toString))
        } else {
          val actualSeq   = productOrRowToSeq(actualRow)
          val expectedSeq = productOrRowToSeq(expectedRow)
          if (actualSeq.isEmpty)
            List(Red(emptyProd), Green(prodToString(expectedSeq)))
          else if (expectedSeq.isEmpty)
            List(Red(prodToString(actualSeq)), Green(emptyProd))
          else {
            val withEquals = actualSeq
              .zipAll(expectedSeq, "MISSING", "MISSING")
              .map { case (actualRowField, expectedRowField) =>
                (actualRowField, expectedRowField, actualRowField == expectedRowField)
              }
            val allFieldsAreNotEqual = !withEquals.exists(_._3)
            if (allFieldsAreNotEqual) {
              List(Red(prodToString(actualSeq)), Green(prodToString(expectedSeq)))
            } else {
              val coloredDiff = withEquals.map {
                case (actualRowField, expectedRowField, true) =>
                  (DarkGray(rowFieldToString(actualRowField)), DarkGray(rowFieldToString(expectedRowField)))
                case (actualRowField, expectedRowField, false) =>
                  (Red(rowFieldToString(actualRowField)), Green(rowFieldToString(expectedRowField)))
              }
              val start = DarkGray(s"$className$lBracket")
              val sep   = DarkGray(",")
              val end   = DarkGray(rBracket)
              List(
                coloredDiff.map(_._1).mkStr(start, sep, end),
                coloredDiff.map(_._2).mkStr(start, sep, end)
              )
            }
          }
        }
      }
    val numCols = 2

    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minColWidth)

    // Compute the width of each column
    header.zipWithIndex.foreach({ case (cell, i) =>
      colWidths(i) = math.max(colWidths(i), cell.length)
    })

    diff.foreach { row =>
      row.zipWithIndex.foreach { case (cell, i) =>
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
    header.zipWithIndex
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

  private val EmptyStr = Str("")
  private[mrpowers] def showProductDiffSeparateLine[T: ClassTag](
      actual: Seq[T],
      expected: Seq[T],
      fieldNames: Array[String],
      truncate: Int = 20,
      minColWidth: Int = 3
  ): String = {
    val sb           = new StringBuilder
    val actualRows   = actual.map(productOrRowToSeq)
    val expectedRows = expected.map(productOrRowToSeq)
    val colWidths    = getColWidths(fieldNames, actualRows ++ expectedRows, truncate, minColWidth)
    // needed to calculate padding for other elements
    val largestIndexOffset = {
      if (actualRows.isEmpty && expectedRows.isEmpty) 0
      else {
        val maxIndex     = Math.max(actualRows.length, expectedRows.length) - 1
        val largestIndex = maxIndex.toString + ":" // Largest index that is visible on the side. Like `100:`
        largestIndex.length + 1
      }
    }

    val diff = actualRows
      .zipAll(expectedRows, Seq.empty[Seq[Any]], Seq.empty[Seq[Any]])
      .map { case (actualRow, expectedRow) =>
        val rowsAreEqual           = actualRow == expectedRow
        lazy val paddedActualRow   = pad(actualRow, colWidths, truncate)
        lazy val paddedExpectedRow = pad(expectedRow, colWidths, truncate)
        val rowsDiff = if (rowsAreEqual) {
          List(DarkGray(paddedActualRow.mkString("|")), DarkGray(paddedActualRow.mkString("|")))
        } else {
          val actualSeq   = actualRow
          val expectedSeq = expectedRow
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
        (rowsDiff, rowsAreEqual)
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
    diff.zipWithIndex.foreach { case ((actual :: expected :: Nil, areRowsEqual), i) =>
      def appendRow(row: Str, i: Int): Unit = {
        if (row.length > 0) {
          val indexString = StringUtils.leftPad(s"${i + 1}:|", largestIndexOffset)
          sb.append(indexString)
          sb.append(row)
          sb.append(s"|:${i + 1}\n")
        }
      }
      appendRow(actual, i)
      if (!areRowsEqual) {
        appendRow(expected, i)
        if (i < diff.length - 1)
          sb.append(separatorLine)
      } else if (i < diff.length - 1 && !diff(i + 1)._2) { // if current rows are equal and next ones are not
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

  private def getColWidths(fields: Array[String], rows: Seq[Seq[Any]], truncate: Int, minColWidth: Int) = {
    val colWidths = fields.map { field =>
      math.max(minColWidth, DataFramePrettyPrint.cellToString(field, truncate).length)
    }

    rows.foreach { row =>
      row.zipWithIndex.foreach { case (cell, i) =>
        colWidths(i) = math.max(colWidths(i), DataFramePrettyPrint.cellToString(cell, truncate).length)
      }
    }

    colWidths
  }
}
