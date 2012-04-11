package twitter.crawler.metrics

import collection.mutable
import collection.immutable.SortedSet
import SingleDistribution.{cart, LN2, initMap}
import scala.math.log

class SingleDistribution(history: SortedSet[Long]) {
  var counterMap: mutable.Map[Array[Byte], Long] = _
  var total: Long = _
  var obsSize: Int = _

  def computeCounters(from: Long, to: Long, intervals: Array[Long]) = {
    obsSize = intervals.size
    val pattern = new Pattern(from, intervals)

    counterMap = initMap(obsSize)
    total = to - pattern.rightBorder
    var currenObservable: Array[Byte] = null
    var currentMove: Long = 0
    while (pattern.rightBorder <= to) {
      currenObservable = pattern.observe(history)
      currentMove = pattern.minMove(history)
      counterMap.put(currenObservable, counterMap(currenObservable) + currentMove)
      pattern.move(currentMove)
    }
  }

  def computeCondEntropy: Double = {
    var result: Double = 0
    cart(List.make(obsSize - 1, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        var with_1_Count = 1.0 * counterMap((list.::[Byte](1)).toArray) + 1
        var with_0_Count = 1.0 * counterMap((list.::[Byte](0)).toArray) + 1
        var marginalized = with_1_Count + with_0_Count
        result += (with_1_Count / total) * log(with_1_Count / marginalized) / LN2
        result += (with_0_Count / total) * log(with_0_Count / marginalized) / LN2
    }
    result
  }
}

object SingleDistribution {
  def log2(x: Double) = log(x) / LN2

  val LN2 = log(2)

  def cart[T](listOfLists: List[List[T]]): List[List[T]] = listOfLists match {
    case Nil => List(List())
    case xs :: xss => for (y <- xs; ys <- cart(xss)) yield y :: ys
  }

  def initMap(size: Int): mutable.Map[Array[Byte], Long] = {
    val result = mutable.Map[Array[Byte], Long]()
    cart(List.make(size, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        result.put(list.toArray, 0l)
    }
    result
  }

}
