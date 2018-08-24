package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.DataFrame

case class ColumnMismatch(smth: String) extends Exception(smth)

trait ColumnComparer {

  def assertColumnEquality(
      df: DataFrame,
      colName1: String,
      colName2: String
  ): Unit = {
    val elements = df.select(colName1, colName2).collect()
    val colName1Elements = elements.map(_(0))
    val colName2Elements = elements.map(_(1))
    if (!colName1Elements.sameElements(colName2Elements)) {
      val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
        Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
      )
      throw ColumnMismatch(mismatchMessage)
    }
  }

  // ace stands for 'assertColumnEquality'
  def ace(
      df: DataFrame,
      colName1: String,
      colName2: String
  ): Unit = {
    assertColumnEquality(df, colName1, colName2)
  }

  private def approximatelyEqual(x: Double,
                                 y: Double,
                                 precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

  private def areDoubleArraysEqual(x: Array[Double],
                                   y: Array[Double],
                                   precision: Double): Boolean = {
    val zipped: Array[(Double, Double)] = x.zip(y)
    val mapped = zipped.map { t =>
      approximatelyEqual(t._1, t._2, 0.01) == false
    }
    mapped.contains(false)
  }

  def assertDoubleTypeColumnEquality(
      df: DataFrame,
      colName1: String,
      colName2: String,
      precision: Double = 0.01
  ): Unit = {
    val elements = df.select(colName1, colName2).collect()
    val colName1Elements: Array[Double] = elements.map(_(0).toString().toDouble)
    val colName2Elements: Array[Double] = elements.map(_(1).toString().toDouble)
    if (!areDoubleArraysEqual(colName1Elements, colName2Elements, precision)) {
      val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
        Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
      )
      throw ColumnMismatch(mismatchMessage)
    }
  }

}
