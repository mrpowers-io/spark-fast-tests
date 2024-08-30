package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object DataframeUtil {

  def showDataframeDiff(
                         header: (String, String),
                         actual: Array[Row],
                         expected: Array[Row],
                         truncate: Int = 20
                       ): String = {

    val sb = new StringBuilder
    val diff = actual.zip(expected).map { case (a, e) =>
      if (equals(a, e)) {
        List(ufansi.Color.DarkGray(a.toString()), ufansi.Color.DarkGray(e.toString()))
      } else {
        val d = a.toSeq
          .zip(e.toSeq)
          .map { case (a1, e1) =>
            if (a1 == e1)
              (DarkGray(a1.toString()), DarkGray(e1.toString))
            else (Red(a1.toString()), Green(e1.toString))
          }
        List(
          DarkGray("[") ++ d.map(_._1).reduce(_ ++ DarkGray(",") ++ _) ++ DarkGray("]"),
          DarkGray("[") ++ d.map(_._2).reduce(_ ++ DarkGray(",") ++ _) ++ DarkGray("]")
        )
      }
    }
    val rows = Array(List(header._1, header._2))
    val numCols = 2

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
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
        .toString()

    // column names
    val h: Seq[(String, Int)] = rows.head.zipWithIndex
    h.map { case (cell, i) =>
      if (truncate > 0) {
        StringUtils.leftPad(cell, colWidths(i))
      } else {
        StringUtils.rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

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

    sb.toString()
  }


}
