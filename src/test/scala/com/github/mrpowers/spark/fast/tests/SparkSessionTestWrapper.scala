package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    val ss = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    ss
  }

}
