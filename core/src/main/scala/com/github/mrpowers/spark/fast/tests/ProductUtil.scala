package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import com.github.mrpowers.spark.fast.tests.ufansi.FansiExtensions.StrOps
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object ProductUtil {
  private[mrpowers] def productOrRowToSeq(product: Any): Seq[Any] = {
    product match {
      case null       => Seq.empty
      case r: Row     => r.toSeq
      case p: Product => p.productIterator.toSeq
      case _          => throw new IllegalArgumentException("Only Row and Product types are supported")
    }
  }
  private[mrpowers] def showProductDiff[T: ClassTag](
      header: (String, String),
      actual: Seq[T],
      expected: Seq[T],
      truncate: Int = 20,
      minColWidth: Int = 3,
      defaultVal: T = null.asInstanceOf[T],
      border: (String, String) = ("[", "]")
  ): String = {
    val className                        = implicitly[ClassTag[T]].runtimeClass.getSimpleName
    val prodToString: Seq[Any] => String = s => s.mkString(s"$className${border._1}", ",", border._2)
    val emptyProd                        = s"$className()"

    val sb = new StringBuilder

    val fullJoin = actual.zipAll(expected, defaultVal, defaultVal)

    val diff = fullJoin.map { case (actualRow, expectedRow) =>
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
            .zip(expectedSeq)
            .map { case (actualRowField, expectedRowField) =>
              (actualRowField, expectedRowField, actualRowField == expectedRowField)
            }
          val allFieldsAreNotEqual = !withEquals.exists(_._3)
          if (allFieldsAreNotEqual) {
            List(Red(prodToString(actualSeq)), Green(prodToString(expectedSeq)))
          } else {
            val coloredDiff = withEquals
              .map {
                case (actualRowField, expectedRowField, true) =>
                  (DarkGray(actualRowField.toString), DarkGray(expectedRowField.toString))
                case (actualRowField, expectedRowField, false) =>
                  (Red(actualRowField.toString), Green(expectedRowField.toString))
              }
            val start = DarkGray(s"$className${border._1}")
            val sep   = DarkGray(",")
            val end   = DarkGray(border._2)
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
    headerSeq.zipWithIndex.foreach({ case (cell, i) =>
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
