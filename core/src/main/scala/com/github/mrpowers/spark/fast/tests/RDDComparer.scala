package com.github.mrpowers.spark.fast.tests

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class RDDContentMismatch(smth: String) extends Exception(smth)

trait RDDComparer {

  def contentMismatchMessage[T: ClassTag](actualRDD: RDD[T], expectedRDD: RDD[T]): String = {
    s"""
Actual RDD Content:
${actualRDD.take(5).mkString("\n")}
Expected RDD Content:
${expectedRDD.take(5).mkString("\n")}
"""
  }

  def assertSmallRDDEquality[T: ClassTag](actualRDD: RDD[T], expectedRDD: RDD[T]): Unit = {
    if (!actualRDD.collect().sameElements(expectedRDD.collect())) {
      throw RDDContentMismatch(
        contentMismatchMessage(actualRDD, expectedRDD)
      )
    }
  }

}
