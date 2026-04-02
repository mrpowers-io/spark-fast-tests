package com.github.mrpowers.spark.fast.tests

/**
 * Provides convenient imports for Snowpark testing.
 *
 * Usage:
 * {{{
 * import com.github.mrpowers.spark.fast.tests.snowpark._
 *
 * class MyTest extends AnyFunSuite with SnowparkDataFrameComparer {
 *   test("my dataframe test") {
 *     assertSmallDataFrameEquality(actualDF, expectedDF)
 *   }
 * }
 * }}}
 */
package object snowpark {

  // Re-export the main comparer trait
  type SnowparkDataFrameComparer = com.github.mrpowers.spark.fast.tests.SnowparkDataFrameComparer

  // Re-export adapters for advanced usage
  val SnowparkRowAdapter    = com.github.mrpowers.spark.fast.tests.SnowparkRowAdapter
  val SnowparkSchemaAdapter = com.github.mrpowers.spark.fast.tests.SnowparkSchemaAdapter
  val SnowparkDataFrameLike = com.github.mrpowers.spark.fast.tests.SnowparkDataFrameLike
}
