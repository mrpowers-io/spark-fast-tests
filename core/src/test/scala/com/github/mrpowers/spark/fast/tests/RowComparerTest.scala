package com.github.mrpowers.spark.fast.tests

import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.sql.Row

class RowComparerTest extends AnyFreeSpec {

  "areRowsEqual" - {

    "returns true for rows that contain the same elements" in {
      val r1 = Row("a", "b")
      val r2 = Row("a", "b")
      assert(
        RowComparer.areRowsEqual(r1, r2, 0.0)
      )
    }

    "returns false for rows that don't contain the same elements" - {
      val r1 = Row("a", 3)
      val r2 = Row("a", 4)
      assert(
        !RowComparer.areRowsEqual(r1, r2, 0.0)
      )
    }

  }

}
