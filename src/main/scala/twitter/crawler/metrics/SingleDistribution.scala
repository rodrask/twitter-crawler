package twitter.crawler.metrics

import collection.mutable
import collection.immutable.SortedSet
import SingleDistribution.{cart, log2, initMap}
import scala.math.{log => log_math, min}
import com.codahale.logula.Logging

class SingleDistribution(from: Long, to: Long, history: SortedSet[Long]) extends Logging {
  var counterMap: mutable.Map[List[Byte], Long] = _
  var total: Long = _

  def computeCounters(intervals: List[Long]): mutable.Map[List[Byte], Long] = {

    val obsSize = intervals.size
    val pattern = new Pattern(from, intervals)

    counterMap = initMap(obsSize)
    total = 0
    var currenObservable: List[Byte] = null
    var currentMove: Long = 0
    while (pattern.isInside(to)) {
      currenObservable = pattern.observe(history)
      currentMove = pattern.minMove(history)
      counterMap.put(currenObservable, counterMap(currenObservable) + currentMove)
      total += currentMove
      pattern.move(currentMove)
    }
    counterMap
  }
}

object SingleDistribution {
  def log2(x: Double) = log_math(x) / LN2

  val LN2 = log_math(2)

  def cart[T](listOfLists: List[List[T]]): List[List[T]] = listOfLists match {
    case Nil => List(List())
    case xs :: xss => for (y <- xs; ys <- cart(xss)) yield y :: ys
  }

  def initMap(size: Int): mutable.Map[List[Byte], Long] = {
    val result = mutable.Map[List[Byte], Long]()
    cart(List.make(size, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        result.put(list, 0l)
    }
    result
  }

  def dumpDistrMap(distr: mutable.Map[List[Byte], Long]) = {
    val size = distr.keys.head.size
    var total = 0l
    cart(List.make(size, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        total += distr(list)
        println(list + " -> " + distr(list))
    }
    println("Total: " + total)
  }

  val z0 = List[Byte](0)
  val z1 = List[Byte](1)

  def computeCondEntropy(counters: mutable.Map[List[Byte], Long], total: Long): Double = {
    val obsSize = counters.keys.head.size
    var result: Double = 0
    cart(List.make(obsSize - 1, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        var with_1_Count = counters((list ::: z1)).toDouble
        var with_0_Count = counters((list ::: z0)).toDouble
        var marginalized = with_1_Count + with_0_Count
        if (with_1_Count > 0)
          result += (with_1_Count / total) * log2(with_1_Count / marginalized)
        if (with_0_Count > 0)
          result += (with_0_Count / total) * log2(with_0_Count / marginalized)
    }
    -result
  }
}

class JoinedDistribution(from: Long, to: Long, YHistory: SortedSet[Long], XHistory: SortedSet[Long]) {
  var counterMap: mutable.Map[List[Byte], Long] = _
  var total: Long = _

  def computeCounters(intervals: List[Long]): mutable.Map[List[Byte], Long] = {
    val obsSize = intervals.size

    val YPattern = new Pattern(from, intervals.take(obsSize - 1))
    val XPattern = new Pattern(from, intervals)

    counterMap = initMap(obsSize * 2 - 1)
    total = 0
    var currentMove: Long = 0
    var concatObs: List[Byte] = null

    while (XPattern.isInside(to)) {
      concatObs = YPattern.observe(YHistory) ::: XPattern.observe(XHistory)
      currentMove = min(XPattern.minMove(XHistory), YPattern.minMove(YHistory))
      counterMap.put(concatObs, counterMap(concatObs) + currentMove)
      total += currentMove
      XPattern.move(currentMove)
      YPattern.move(currentMove)
    }
    counterMap
  }
}



