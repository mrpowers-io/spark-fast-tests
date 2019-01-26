//package com.github.mrpowers.spark.fast.tests
//
//import org.apache.spark.sql.types._
//import SparkSessionExt._
//
//import utest._
//
//object ExamplesTest extends TestSuite with SparkSessionTestWrapper with DataFrameComparer {
//
//  val tests = Tests {
//
//    "error when row counts don't match" - {
//
//      val sourceDF = spark.createDF(
//        List(
//          (1),
//          (5)
//        ),
//        List(("number", IntegerType, true))
//      )
//
//      val expectedDF = spark.createDF(
//        List(
//          (1),
//          (5),
//          (10)
//        ),
//        List(("number", IntegerType, true))
//      )
//
//      assertSmallDatasetEquality(
//        sourceDF,
//        expectedDF
//      )
//
//    }
//
//  }
//
//}
