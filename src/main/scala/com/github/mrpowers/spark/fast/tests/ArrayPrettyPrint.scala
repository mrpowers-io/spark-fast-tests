package com.github.mrpowers.spark.fast.tests

import java.sql.Date

import com.github.mrpowers.spark.fast.tests.ufansi.EscapeAttr
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils

object ArrayPrettyPrint {

  def weirdTypesToStrings(arr: Array[(Any, Any)], truncate: Int = 20): Array[List[String]] = {
    arr.map { row =>
      row.productIterator.toList.map { cell =>
        val str = cell match {
          case null                => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_]     => array.mkString("[", ", ", "]")
          case seq: Seq[_]         => seq.mkString("[", ", ", "]")
          case d: Date             => DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
          case _                   => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4)
            str.substring(0, truncate)
          else
            str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }
    }
  }

  def showTwoColumnString(arr: Array[(Any, Any)], truncate: Int = 20): String = {
    val rows    = weirdTypesToStrings(arr, truncate)
    val numCols = 2

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    val sb = new StringBuilder

    // Create SeparateLine
    val sep: String =
      colWidths
        .map("-" * _)
        .addString(sb, "+", "+", "+\n")
        .toString()

    // column names
    val h: Seq[(String, Int)] = rows.head.zipWithIndex
    h.map {
        case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell, colWidths(i))
          } else {
            StringUtils.rightPad(cell, colWidths(i))
          }
      }
      .addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map { row =>
      val color = if (row(0) == row(1)) "blue" else "red"
      row.zipWithIndex
        .map {
          case (cell, i) =>
            val r = if (truncate > 0) {
              StringUtils.leftPad(cell.toString, colWidths(i))
            } else {
              StringUtils.rightPad(cell.toString, colWidths(i))
            }
            if (color == "blue") {
              ufansi.Color.DarkGray(r)
            } else {
              ufansi.Color.Red(r)
            }
        }
        .addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    sb.toString()
  }

  def showTwoColumnStringColorCustomizable(
      arr: Array[(Any, Any)],
      rowEqual: Array[Boolean],
      truncate: Int = 20,
      equalColor: EscapeAttr = ufansi.Color.Blue,
      unequalColor: EscapeAttr = ufansi.Color.Red
  ): String = {
    val sb      = new StringBuilder
    val numCols = 2
    val rows    = weirdTypesToStrings(arr, truncate)

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String =
      colWidths
        .map("-" * _)
        .addString(sb, "+", "+", "+\n")
        .toString()

    // column names
    val h: Seq[(String, Int)] = rows.head.zipWithIndex
    h.map {
        case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell, colWidths(i))
          } else {
            StringUtils.rightPad(cell, colWidths(i))
          }
      }
      .addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.zipWithIndex.map {
      case (row, j) =>
        row.zipWithIndex
          .map {
            case (cell, i) =>
              val r = if (truncate > 0) {
                StringUtils.leftPad(cell.toString, colWidths(i))
              } else {
                StringUtils.rightPad(cell.toString, colWidths(i))
              }
              if (rowEqual(j)) {
                equalColor(r)
              } else {
                unequalColor(r)
              }
          }
          .addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    sb.toString()
  }

}
