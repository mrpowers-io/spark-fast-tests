package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.{Column, DataFrame}

case class ColumnMismatch(smth: String) extends Exception(smth)

trait ColumnComparer {

  def assertColumnEquality(
    df: DataFrame,
    colName1: String,
    colName2: String
  ): Unit = {
    val colName1Elements = df.select(colName1).collect()
    val colName2Elements = df.select(colName2).collect()
    val mismatchMessage = s"""
Columns aren't equal
colName1 Elements:
${colName1Elements.mkString(", ")}
colName2 Elements:
${colName2Elements.mkString(", ")}
"""
    if (!colName1Elements.sameElements(colName2Elements)) {
      throw ColumnMismatch(mismatchMessage)
    }
  }

}
