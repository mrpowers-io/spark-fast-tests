package com.github.mrpowers.spark.fast.tests

import org.scalatest.freespec.AnyFreeSpec
import SeqLikesExtensions._

class SeqLikesExtensionsTest extends AnyFreeSpec with SparkSessionTestWrapper {

  "check equality" - {
    import spark.implicits._

    "check equal Seq" in {
      val source = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      )

      val expected = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      )

      assert(source.approximateSameElements(expected, (s1, s2) => s1 == s2))
    }

    "check equal Seq[Row]" in {

      val source = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      ).toDF.collect().toSeq

      val expected = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      ).toDF.collect()

      assert(source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _)))
    }

    "check unequal Seq[Row]" in {

      val source = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      ).toDF.collect().toSeq

      val expected = Seq(
        ("juan", 5),
        ("bob", 1),
        ("li", 40),
        ("alice", 5)
      ).toDF.collect()

      assert(!source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _)))
    }

    "check equal Seq[Row] with tolerance" in {
      val source = Seq(
        ("juan", 12.00000000001),
        ("bob", 1.00000000001),
        ("li", 49.00000000001),
        ("alice", 5.00000000001)
      ).toDF.collect().toSeq

      val expected = Seq(
        ("juan", 12.0),
        ("bob", 1.0),
        ("li", 49.0),
        ("alice", 5.0)
      ).toDF.collect()
      assert(source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _, 0.0000000002)))
    }

    "check indexedSeq[Row] with tolerance" in {
      val source = Seq(
        ("juan", 12.00000000001),
        ("bob", 1.00000000001),
        ("li", 49.00000000001),
        ("alice", 5.00000000001)
      ).toDF.collect().toIndexedSeq

      val expected = Seq(
        ("juan", 12.0),
        ("bob", 1.0),
        ("li", 49.0),
        ("alice", 5.0)
      ).toDF.collect().toIndexedSeq

      assert(source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _, 0.0000000002)))
    }

    "check non equal Seq[Row] with tolerance" in {
      val source = Seq(
        ("juan", 12.00000000002),
        ("bob", 1.00000000002),
        ("li", 49.00000000002),
        ("alice", 5.00000000002)
      ).toDF.collect().toSeq

      val expected = Seq(
        ("juan", 12),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      ).toDF.collect()

      assert(!source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _, .00000000001)))
    }

    "check non equal indexedSeq[Row] with tolerance" in {
      val source = Seq(
        ("juan", 12.00000000002),
        ("bob", 1.00000000002),
        ("li", 49.00000000002),
        ("alice", 5.00000000002)
      ).toDF.collect().toIndexedSeq

      val expected = Seq(
        ("juan", 12),
        ("bob", 1),
        ("li", 49),
        ("alice", 5)
      ).toDF.collect().toIndexedSeq

      assert(!source.approximateSameElements(expected, RowComparer.areRowsEqual(_, _, .00000000001)))
    }
  }
}