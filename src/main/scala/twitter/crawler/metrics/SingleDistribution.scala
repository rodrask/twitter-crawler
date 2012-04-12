package twitter.crawler.metrics

import collection.mutable
import collection.immutable.SortedSet
import SingleDistribution.{cart, log2, initMap}
import scala.math.{log => log_math}
import com.codahale.logula.Logging

class SingleDistribution(history: SortedSet[Long]) extends Logging{
  var counterMap: mutable.Map[List[Byte], Long] = _
  var total: Long = _
  var obsSize: Int = _

  def computeCounters(from: Long, to: Long, intervals: List[Long]) = {
    obsSize = intervals.size
    val pattern = new Pattern(from, intervals)

    counterMap = initMap(obsSize)
    total = to - pattern.rightBorder
    var currenObservable: List[Byte] = null
    var currentMove: Long = 0
    while (pattern.rightBorder <= to) {
//      println("Pattern "+pattern.borders)
      currenObservable = pattern.observe(history)
//      println("Obs "+currenObservable)
      currentMove = pattern.minMove(history)
//      println(currentMove)
      counterMap.put(currenObservable, counterMap(currenObservable) + currentMove)
      pattern.move(currentMove)
    }
  }

  def computeCondEntropy: Double = {
    var result: Double = 0
    cart(List.make(obsSize - 1, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        var with_1_Count = 1.0 * counterMap((list.::[Byte](1)))
        var with_0_Count = 1.0 * counterMap((list.::[Byte](0)))
        var marginalized = with_1_Count + with_0_Count
        if (with_1_Count > 0)
          result += (with_1_Count / total) * log2(with_1_Count / marginalized)
        if (with_0_Count > 0)
          result += (with_0_Count / total) * log2(with_0_Count / marginalized)

    }
    -result
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

  def dumpDistrMap(distr: mutable.Map[List[Byte], Long])={
    val size = distr.keys.head.size
    var total = 0l
    cart(List.make(size, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        total += distr(list)
        println(list+" -> "+distr(list))
    }
    println("Total: "+total)
  }

}
