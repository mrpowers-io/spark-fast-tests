package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.{Failure, Success, Try}

case class ColumnMismatch(smth: String) extends Exception(smth)

trait ColumnComparer extends DatasetComparer {

  private def assertColumn[T](
    df: Dataset[T],
    colName1: String,
    colName2: String,
    equals: (Row, Row) => Boolean
  ): Unit = {
    val x = df.select(colName1)
    val y = df.select(colName2)
    val z = Try {
      collectAndAssert(
        x,
        y,
        equals
      )
    }

    z match {
      case Failure(exception) => throw ColumnMismatch(exception.getMessage)
      case Success(_)         =>
    }
  }

  def assertColumnEquality[T](df: Dataset[T], colName1: String, colName2: String): Unit = {
    assertColumn(
      df,
      colName1,
      colName2,
      DatasetComparerLike.naiveEquality
    )
  }

  // ace stands for 'assertColumnEquality'
  def ace[T](df: Dataset[T], colName1: String, colName2: String): Unit = {
    assertColumnEquality(
      df,
      colName1,
      colName2
    )
  }

  def assertDoubleTypeColumnEquality[T](
    df: Dataset[T],
    colName1: String,
    colName2: String,
    precision: Double = 0.01
  ): Unit = {
    assertColumn(
      df,
      colName1,
      colName2,
      DatasetComparerLike.precisionEquality(
        _,
        _,
        precision
      )
    )
  }
}
