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
    val mismatchMessage = s"""
Columns aren't equal
'$colName1' elements:
[${colName1Elements.mkString(", ")}]
'$colName2' elements:
[${colName2Elements.mkString(", ")}]
"""
    if (!colName1Elements.sameElements(colName2Elements)) {
      throw ColumnMismatch(mismatchMessage)
    }
  }

}
