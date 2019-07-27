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
//    'assertSmallDatasetEquality - {
//
//      "error when row counts don't match" - {
//
//        val sourceDF = spark.createDF(
//          List(
//            (1),
//            (5)
//          ),
//          List(("number", IntegerType, true))
//        )
//
//        val expectedDF = spark.createDF(
//          List(
//            (1),
//            (5),
//            (10)
//          ),
//          List(("number", IntegerType, true))
//        )
//
//        assertSmallDatasetEquality(
//          sourceDF,
//          expectedDF
//        )
//
//      }
//
//      "error when schemas don't match" - {
//
//        val sourceDF = spark.createDF(
//          List(
//            (1, "a"),
//            (5, "b")
//          ),
//          List(
//            ("number", IntegerType, true),
//            ("letter", StringType, true)
//          )
//        )
//
//        val expectedDF = spark.createDF(
//          List(
//            (1, "a"),
//            (5, "b")
//          ),
//          List(
//            ("num", IntegerType, true),
//            ("letter", StringType, true)
//          )
//        )
//
//        assertSmallDatasetEquality(
//          sourceDF,
//          expectedDF
//        )
//
//      }
//
//      "error when content doesn't match" - {
//
//        val sourceDF = spark.createDF(
//          List(
//            (1, "z"),
//            (5, "b")
//          ),
//          List(
//            ("number", IntegerType, true),
//            ("letter", StringType, true)
//          )
//        )
//
//        val expectedDF = spark.createDF(
//          List(
//            (1, "a"),
//            (5, "b")
//          ),
//          List(
//            ("number", IntegerType, true),
//            ("letter", StringType, true)
//          )
//        )
//
//        assertLargeDataFrameEquality(
//          sourceDF,
//          expectedDF
//        )
//
//      }
//
//    }
//
//  }
//
//}
