package com.github.mrpowers.spark.fast.tests

import java.sql.Date
import com.github.mrpowers.spark.fast.tests.ufansi.EscapeAttr
import java.time.format.DateTimeFormatter
import org.apache.commons.lang3.StringUtils

object ArrayUtil {

  def weirdTypesToStrings(arr: Array[(Any, Any)], truncate: Int = 20): Array[List[String]] = {
    arr.map { row =>
      row.productIterator.toList.map { cell =>
        val str = cell match {
          case null                => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_]     => array.mkString("[", ", ", "]")
          case seq: Seq[_]         => seq.mkString("[", ", ", "]")
          case d: Date             => d.toLocalDate.format(DateTimeFormatter.ISO_DATE)
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

  // The following code is taken from: https://github.com/scala/scala/blob/86e75db7f36bcafdd75302f2c2cca0c68413214d/src/partest/scala/tools/partest/Util.scala
  def prettyArray(a: Array[_]): collection.IndexedSeq[Any] = new collection.AbstractSeq[Any] with collection.IndexedSeq[Any] {
    def length: Int = a.length

    def apply(idx: Int): Any = a(idx) match {
      case x: AnyRef if x.getClass.isArray => prettyArray(x.asInstanceOf[Array[_]])
      case x                               => x
    }

    // Ignore deprecation warning in 2.13 - this is the correct def for 2.12 compatibility
    override def stringPrefix: String = "Array"
  }

  // The following code is taken from: https://github.com/scala/scala/blob/86e75db7f36bcafdd75302f2c2cca0c68413214d/src/partest/scala/tools/partest/Util.scala
  implicit class ArrayDeep(val a: Array[_]) extends AnyVal {
    def deep: collection.IndexedSeq[Any] = prettyArray(a)
  }
}
