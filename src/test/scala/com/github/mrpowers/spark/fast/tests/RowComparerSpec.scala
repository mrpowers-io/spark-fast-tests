package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import org.scalatest.FunSpec

class RowComparerSpec extends FunSpec {

  describe(".areRowsEqual") {

    it("returns true for rows that contain the same elements") {

      val r1 = Row("a", "b")
      val r2 = Row("a", "b")
      assert(RowComparer.areRowsEqual(r1, r2, 0.0) === true)

    }

    it("returns false for rows that don't contain the same elements") {

      val r1 = Row("a", 3)
      val r2 = Row("a", 4)
      assert(RowComparer.areRowsEqual(r1, r2, 0.0) === false)

    }

  }

}
