package com.github.mrpowers.spark.fast.tests

import org.apache.commons.math3.util.Precision
import org.apache.spark.sql.Row

import scala.math.abs

object RowComparer {

  /** Approximate equality, based on equals from [[Row]] */
  def areRowsEqual(r1: Row, r2: Row, tol: Double = 0): Boolean = {
    if (tol == 0) {
      return r1 == r2
    }
    if (r1.length != r2.length) {
      return false
    }
    for (i <- 0 until r1.length) {
      if (r1.isNullAt(i) != r2.isNullAt(i)) {
        return false
      }
      if (!r1.isNullAt(i)) {
        val o1 = r1.get(i)
        val o2 = r2.get(i)
        val valid = o1 match {
          case b1: Array[Byte] =>
            o2.isInstanceOf[Array[Byte]] && java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])
          case f1: Float if o2.isInstanceOf[Float]   => Precision.equalsIncludingNaN(f1, o2.asInstanceOf[Float], tol)
          case d1: Double if o2.isInstanceOf[Double] => Precision.equalsIncludingNaN(d1, o2.asInstanceOf[Double], tol)
          case bd1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
            bd1.subtract(o2.asInstanceOf[java.math.BigDecimal]).abs().compareTo(new java.math.BigDecimal(tol)) == -1
          case f1: Number if o2.isInstanceOf[Number] =>
            val bd1 = new java.math.BigDecimal(f1.toString)
            val bd2 = new java.math.BigDecimal(o2.toString)
            bd1.subtract(bd2).abs().compareTo(new java.math.BigDecimal(tol)) == -1
          case t1: java.sql.Timestamp           => abs(t1.getTime - o2.asInstanceOf[java.sql.Timestamp].getTime) > tol
          case t1: java.time.Instant            => abs(t1.toEpochMilli - o2.asInstanceOf[java.time.Instant].toEpochMilli) > tol
          case rr1: Row if o2.isInstanceOf[Row] => areRowsEqual(rr1, o2.asInstanceOf[Row], tol)
          case _                                => o1 == o2
        }
        if (!valid) {
          return false
        }
      }
    }
    true
  }
}
