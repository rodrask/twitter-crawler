package twitter.crawler.metrics

import collection.immutable.SortedSet
import collection.mutable

class History(history: List[Int]) {
  val observedValue = history.last
  val histValues = history take (history.size - 1)
}

class MutualHistory(history: List[Int], influencedHistory: List[Int]) {
  val observedValue = history.last
  val histValues = history take (history.size - 1)
  val influencedHistValues = influencedHistory
}

class Pattern(var borders: List[Long]) {
  val size = borders.size

  def getProjectedHistory(history: SortedSet[Long], influenced: Boolean = false): List[Int] = if (influenced) {
    borders.sliding(2).take(size - 2).map(interval => hasInside(interval, history)).toList
  }
  else {
    borders.sliding(2).map(interval => hasInside(interval, history)).toList
  }

  def hasInside(interval: List[Long], history: SortedSet[Long]): Int = {
    if (history.range(interval(0), interval(1)).size > 0) 1 else 0
  }

  def hopLength(history: SortedSet[Long], influenced: Boolean = false) = if (influenced) {
    borders.take(size - 1).map(b => history.from(b + 1).head - b).min
  }
  else {
    borders.map(b => history.from(b + 1).head - b).min
  }

  def doHop(length: Long) = {
    borders = borders map (b => b + length)
  }

  override def toString = {
    borders.toString()
  }

  def initMap = {
    val result = mutable.Map[History, Long]()
    Pattern.cart(List.make(borders.size - 1, List(0, 1))) foreach {
      list: List[Int] =>
        result.put(new History(list), 0l)
    }
    result
  }

  def initInfluencedMap = {
    val result = mutable.Map[MutualHistory, Long]()
    Pattern.cart(List.make(borders.size - 1, List(0, 1))) foreach {
      list: List[Int] =>
        Pattern.cart(List.make(borders.size - 2, List(0, 1))) foreach {
          mList =>
            result.put(new MutualHistory(list, mList), 0l)
        }
    }
    result
  }

  def getStatistics(history: SortedSet[Long], to: Long): mutable.Map[List[Int], Long] = {
    val result = initMap
    var hist: List[Int] = null
    var nextHop: Long = 0
    while (borders.last <= to) {
      hist = getProjectedHistory(history)
      nextHop = hopLength(history)
      result.put(hist, nextHop + result(hist))
      doHop(nextHop)
    }
    result
  }

  def getStatistics(history: SortedSet[Long], influecedHistory: SortedSet[Long], to: Long): mutable.Map[List[Int], Long] = {
    val result = initMap
    var hist: History = null
    var nextHop: Long = 0
    while (borders.last <= to) {
      hist = new History(getProjectedHistory(history))
      nextHop = hopLength(history)
      result.put(hist, nextHop + result(hist))
      doHop(nextHop)
    }
    result
  }
}

object Pattern {
  def makeBorders(from: Long, intervals: List[Long]): List[Long] = {
    intervals.scanLeft(from)(_ + _)
  }

  def cart[T](listOfLists: List[List[T]]): List[List[T]] = listOfLists match {
    case Nil => List(List())
    case xs :: xss => for (y <- xs; ys <- cart(xss)) yield y :: ys
  }
}

object ITCalculator {
  val SECOND = 1
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS = List[Long](DAY, 2 * HOUR, 10 * MINUTE, SECOND)
  type Period = (Long, Long)

  def calculateIT(fromUserTs: List[Long], toUserTs: List[Long], period: Period): Double = {
    0
  }


}
