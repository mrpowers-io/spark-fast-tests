package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

case class ColumnMismatch(smth: String) extends Exception(smth)

trait ColumnComparer {

  def assertColumnEquality(df: DataFrame, colName1: String, colName2: String): Unit = {
    val elements = df
      .select(colName1, colName2)
      .collect()
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
  def ace(df: DataFrame, colName1: String, colName2: String): Unit = {
    assertColumnEquality(df, colName1, colName2)
  }

  private def approximatelyEqualDouble(x: Double, y: Double, precision: Double): Boolean = {
    (x - y).abs < precision
  }

  private def areDoubleArraysEqual(x: Array[Double], y: Array[Double], precision: Double): Boolean = {
    x.zip(y).forall { t =>
      approximatelyEqualDouble(t._1, t._2, precision)
    }
  }

  def assertDoubleTypeColumnEquality(df: DataFrame, colName1: String, colName2: String, precision: Double = 0.01): Unit = {
    val elements: Array[Row] = df
      .select(colName1, colName2)
      .collect()
    var areEqual = true
    for (i <- 0 until elements.length) {
      var t = elements(i)
      if (t(0) == null && t(1) == null) {} else if (t(0) != null && t(1) == null) {
        areEqual = false
      } else if (t(0) == null && t(1) != null) {
        areEqual = false
      } else {
        if (!approximatelyEqualDouble(t(0).toString.toDouble, t(1).toString.toDouble, precision)) {
          areEqual = false
        }
      }
    }
    if (areEqual == false) {
      val colName1Elements = elements.map(_(0))
      val colName2Elements = elements.map(_(1))
      val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
        Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
      )
      throw ColumnMismatch(mismatchMessage)
    }
  }

  private def approximatelyEqualFloat(x: Float, y: Float, precision: Float): Boolean = {
    (x - y).abs < precision
  }

  private def areFloatArraysEqual(x: Array[Float], y: Array[Float], precision: Float): Boolean = {
    x.zip(y).forall { t =>
      approximatelyEqualFloat(t._1, t._2, precision)
    }
  }

  def assertFloatTypeColumnEquality(df: DataFrame, colName1: String, colName2: String, precision: Float): Unit = {
    val elements = df
      .select(colName1, colName2)
      .collect()
    val colName1Elements: Array[Float] = elements.map(_(0).toString.toFloat)
    val colName2Elements: Array[Float] = elements.map(_(1).toString.toFloat)
    if (!areFloatArraysEqual(colName1Elements, colName2Elements, precision)) {
      val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
        Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
      )
      throw ColumnMismatch(mismatchMessage)
    }
  }

}
