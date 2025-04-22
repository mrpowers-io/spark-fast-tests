package com.github.mrpowers.spark.fast.tests

import org.scalatest.freespec.AnyFreeSpec

class DataFramePrettyPrintTest extends AnyFreeSpec with SparkSessionTestWrapper {
  "prints named_struct with keys" in {
    val inputDataframe = spark.sql("""select 1 as id, named_struct("k1", 2, "k2", 3) as to_show_with_k""")
    assert(
      DataFramePrettyPrint.showString(inputDataframe, 10) ==
        """+---+------------------+
        || id|    to_show_with_k|
        |+---+------------------+
        ||  1|{k1 -> 2, k2 -> 3}|
        |+---+------------------+
        |""".stripMargin
    )
  }
}
