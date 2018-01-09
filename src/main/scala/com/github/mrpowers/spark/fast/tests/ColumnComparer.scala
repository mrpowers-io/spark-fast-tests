package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

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
    val mismatchMessage =
      s"""
Columns aren't equal
'$colName1' elements:
[${colName1Elements.mkString(", ")}]
'$colName2' elements:
[${colName2Elements.mkString(", ")}]
"""

    val zippedArrays = colName1Elements.zip(colName2Elements)

    def compareSets(a: mutable.WrappedArray[_], b: mutable.WrappedArray[_]) = {
      val equalSets = a.toSet == b.toSet
      if (!equalSets) {
        throw ColumnMismatch(mismatchMessage)
      }
    }

    val isNotArrayElement = zippedArrays.map((tuple) => {
      val comparingArrayElements = tuple._1 match {
        case a: mutable.WrappedArray[_] =>
          tuple._2 match {
            case b: mutable.WrappedArray[_] =>
              compareSets(a, b)
            case _ => throw ColumnMismatch(mismatchMessage)
          }
          false
        case a if Option(a).isEmpty && Option(tuple._2).isDefined || Option(tuple._2).isEmpty && Option(a).isDefined =>

          throw ColumnMismatch(mismatchMessage)
        case a if Option(a).isEmpty && Option(tuple._2).isEmpty => false
        case _ =>

          true

      }
      comparingArrayElements
    }).exists(a => a)

    if (isNotArrayElement && !colName1Elements.sameElements(colName2Elements)) {
      throw ColumnMismatch(mismatchMessage)
    }

  }

}
