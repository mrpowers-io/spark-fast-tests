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
      val mismatchMessage = "\n" +
        s"$colName1 | $colName2\n" +
        colName1Elements.zip(colName2Elements).map {
          case (e1, e2) =>
            // using == insead of equals() per this thread: https://stackoverflow.com/questions/34908289/is-it-safe-to-use-for-comparison-with-null
            if (e1 == e2) {
              ufansi.Color.Blue(s"$e1 | $e2")
            } else {
              ufansi.Color.Red(s"$e1 | $e2")
            }
        }.mkString("\n")
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
