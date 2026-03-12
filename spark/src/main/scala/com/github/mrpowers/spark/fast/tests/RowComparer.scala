package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api.RowLikeComparer
import org.apache.spark.sql.Row

/**
 * Row comparison utilities for Spark.
 */
object RowComparer {

  /** Compares two Rows for approximate equality within the given tolerance. */
  def areRowsEqual(r1: Row, r2: Row, tol: Double = 0): Boolean = {
    RowLikeComparer.areRowsEqual(SparkRowAdapter(r1), SparkRowAdapter(r2), tol)
  }
}
