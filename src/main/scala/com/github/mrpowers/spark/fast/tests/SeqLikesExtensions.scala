package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.Row

import scala.util.Try

object SeqLikesExtensions {
  implicit class SeqExtensions[T](val seq1: Seq[T]) extends AnyVal {
    def approximateSameElements(seq2: Seq[T], equals: (T, T) => Boolean): Boolean = (seq1, seq2) match {
      case (i1: IndexedSeq[_], i2: IndexedSeq[_]) =>
        val length = i1.length
        var equal  = length == i2.length
        if (equal) {
          var index = 0
          val maxApplyCompare = {
            val preferredLength =
              Try(System.getProperty("scala.collection.immutable.IndexedSeq.defaultApplyPreferredMaxLength", "64").toInt).getOrElse(64)
            if (length > (preferredLength.toLong << 1)) preferredLength else length
          }
          while (index < maxApplyCompare && equal) {
            equal = equals(i1(index), i2(index))
            index += 1
          }
          if ((index < length) && equal) {
            val thisIt = i1.iterator.drop(index)
            val thatIt = i2.iterator.drop(index)
            while (equal && thisIt.hasNext) {
              equal = equals(thisIt.next(), thatIt.next())
            }
          }
        }
        equal
      case _ =>
        val thisKnownSize = getKnownSize(seq1)
        val knownSizeDifference = thisKnownSize != -1 && {
          val thatKnownSize = getKnownSize(seq2)
          thatKnownSize != -1 && thisKnownSize != thatKnownSize
        }
        if (knownSizeDifference) {
          return false
        }
        val these = seq1.iterator
        val those = seq2.iterator
        while (these.hasNext && those.hasNext)
          if (!equals(these.next(), those.next()))
            return false
        these.hasNext == those.hasNext
    }

    // scala2.13 optimization: check number of element if it can be cheaply computed
    private def getKnownSize(s: Seq[T]): Int = Try(s.getClass.getMethod("knownSize").invoke(s).asInstanceOf[Int]).getOrElse(s.length)

    def asRows: Seq[Row] = seq1.map {
      case x: Row     => x
      case y: Product => Row(y.productIterator.toSeq: _*)
      case a          => Row(a)
    }
  }
}
