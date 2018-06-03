package com.github.mrpowers.spark.fast.tests

import org.apache.spark.rdd.RDD

object RddHelpers {

  /**
   * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
   * Rows together regardless of if the source is different but still compare based on
   * the order.
   */
  def zipWithIndex[T](rdd: RDD[T]): RDD[(Long, T)] = {
    rdd.zipWithIndex().map {
      case (row, idx) =>
        (idx, row)
    }
  }

}
