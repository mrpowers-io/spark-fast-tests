package com.github.mrpowers.spark.fast.tests.ufansi
object FansiExtensions {
  implicit class StrOps(c: Seq[Str]) {
    def mkStr(start: Str, sep: Str, end: Str): Str =
      c.foldLeft(start)((leftAcc: Str, e: Str) => {
        val separator = if (leftAcc == start) Str("") else sep
        leftAcc ++ separator ++ e
      }) ++ end
  }
}