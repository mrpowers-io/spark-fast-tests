package com.github.mrpowers.spark.fast.tests.ufansi
object FansiExtensions {
  private[mrpowers] implicit class StrOps(c: Seq[Str]) {
    def mkStr(start: Str, sep: Str, end: Str): Str =
      start ++ c.reduce(_ ++ sep ++ _) ++ end
  }
}
