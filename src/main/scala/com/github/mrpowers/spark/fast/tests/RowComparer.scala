package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row
import scala.math.abs

object RowComparer {

  def areRowsEqual(r1: Row, r2: Row, tol: Double): Boolean = {

    if (r1.length != r2.length) {
      return false
    } else {
      var idx = 0
      val length = r1.length
      while (idx < length) {
        if (r1.isNullAt(idx) != r2.isNullAt(idx))
          return false

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) return false

            case f1: Float =>
              if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(o2.asInstanceOf[Float])) return false
              if (abs(f1 - o2.asInstanceOf[Float]) > tol) return false

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(o2.asInstanceOf[Double])) return false
              if (abs(d1 - o2.asInstanceOf[Double]) > tol) return false

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) return false

            case _ =>
              if (o1 != o2) return false
          }
        }
        idx += 1
      }
    }
    true

  }

}
