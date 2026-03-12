package com.github.mrpowers.spark.fast.tests

import org.apache.commons.lang3.StringUtils
import com.github.mrpowers.spark.fast.tests.api.ProductLikeUtil.cellToString

import org.apache.spark.sql.{DataFrame, Row}

object DataFramePrettyPrint {

  def showString(df: DataFrame, _numRows: Int, truncate: Int = 20): String = {
    val numRows     = _numRows.max(0)
    val takeResult  = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data        = takeResult.take(numRows)

    val rows: Seq[Seq[String]] = df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { v =>
        val rowVal = v match {
          case r: Row => SparkRowAdapter(r)
          case other  => other
        }
        cellToString(rowVal, truncate)
      }
    }

    val sb      = new StringBuilder
    val numCols = df.schema.fieldNames.length

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(
          colWidths(i),
          cell.length
        )
      }
    }

    // Create SeparateLine
    val sep: String =
      colWidths
        .map("-" * _)
        .addString(
          sb,
          "+",
          "+",
          "+\n"
        )
        .toString()

    // column names
    val h: Seq[(String, Int)] = rows.head.zipWithIndex
    h.map { case (cell, i) =>
      if (truncate > 0) {
        StringUtils.leftPad(
          cell,
          colWidths(i)
        )
      } else {
        StringUtils.rightPad(
          cell,
          colWidths(i)
        )
      }
    }.addString(
      sb,
      "|",
      "|",
      "|\n"
    )

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex
        .map { case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(
              cell,
              colWidths(i)
            )
          } else {
            StringUtils.rightPad(
              cell,
              colWidths(i)
            )
          }
        }
        .addString(
          sb,
          "|",
          "|",
          "|\n"
        )
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}
