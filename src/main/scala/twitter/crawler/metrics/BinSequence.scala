package twitter.crawler.metrics

import collection.immutable.{BitSet, SortedSet}


class BinSequence(start: Long, end: Long, intervals: List[Long]) {
  val bins: Map[Bin, Int] = intervals.scanLeft(start)(_ + _).sliding(2).map(BinSequence.fromList).zipWithIndex.toMap
  val lastBin: Bin = bins.maxBy[Int] {
    case (b, index) => index
  }._1
  var finished = false

  def count(history: SortedSet[Long]): BitSet = {
    BitSet(bins.filterKeys(bin => bin.check(history)).values.toList: _*)
  }

  def count(history: SortedSet[Long], shift: Int): BitSet = {
    BitSet(bins.filterKeys(bin => bin.check(history)).values.map(_ + shift).toList: _*)
  }

  def minMove(history: SortedSet[Long]): Long = {
    val potentialMove = bins.keys.map(bin => bin.moveToChange(history)).min
    if (potentialMove > end - lastBin.end) {
      finished = true
      end - lastBin.end + 1
    }
    else {
      potentialMove
    }

  }
  def move(distance: Long)={
    bins.keys foreach{
      key =>
        key.move(distance)
    }
  }
}

object BinSequence {
  def fromList(l: List[Long]) = {
    new Bin(l(0), l(1))
  }
}
