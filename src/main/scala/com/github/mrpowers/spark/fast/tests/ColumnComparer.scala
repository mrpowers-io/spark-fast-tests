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

}
