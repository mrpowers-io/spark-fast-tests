package com.github.mrpowers.spark.fast.tests

import scala.annotation.tailrec
import scala.math.abs

import org.apache.spark.sql.Row

object RowComparer {

  /** Approximate equality, based on equals from [[Row]] */
  def areRowsEqual(r1: Row, r2: Row, tol: Double): Boolean = {
    @tailrec
    def compareRowIdx(idx: Int): Boolean = {
      val validation = (r1.get(idx), r2.get(idx)) match {
        case (null, null) => true
        case (null, _) | (_, null) => false
        case (b1: Array[Byte], b2: Array[Byte]) => java.util.Arrays.equals(b1, b2)
        case (f1: Float, f2: Float) =>
          java.lang.Float.isNaN(f1) == java.lang.Float.isNaN(f2) &&
            abs(f1 - f2) <= tol
        case (d1: Double, d2: Double) =>
          java.lang.Double.isNaN(d1) == java.lang.Double.isNaN(d2) &&
            abs(d1 - d2) <= tol
        case (bd1: java.math.BigDecimal, bd2: java.math.BigDecimal) =>
          bd1.subtract(bd2).abs().compareTo(new java.math.BigDecimal(tol)) == -1
        case (t1: java.time.Instant, t2: java.time.Instant) => abs(t1.compareTo(t2)) <= tol
        // TODO: check if this is required for comparing nested row
        case (rr1: Row, rr2: Row) => areRowsEqual(rr1, rr2, tol)
        case (o1, o2) => o1 == o2
      }
      if (validation) {
        compareRowIdx(idx + 1)
      } else {
        false
      }
    }
    r1.length == r2.length && compareRowIdx(0)
  }

}
