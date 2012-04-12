package twitter.crawler.metrics

import collection.immutable.SortedSet
import collection.mutable
import SingleDistribution.{cart, initMap, log2}
import scala.math.min


class JoinedDistribution(YHistory: SortedSet[Long], XHistory: SortedSet[Long]) {
  var counterMap: mutable.Map[List[Byte], Long] = _
  var total: Long = _
  var obsSize: Int = _

  def computeCounters(from: Long, to: Long, intervals: List[Long]) = {
    obsSize = intervals.size

    val YPattern = new Pattern(from, intervals.take(obsSize - 1))
    val XPattern = new Pattern(from, intervals)


    counterMap = initMap(obsSize * 2 - 1)
    total = to - XPattern.rightBorder
    println("total "+total)
    var currentMove: Long = 0
    var concatObs: List[Byte] = null

    while (XPattern.rightBorder <= to) {
      concatObs = YPattern.observe(YHistory) ::: XPattern.observe(XHistory)
      currentMove = min( XPattern.minMove(XHistory), YPattern.minMove(YHistory))
      counterMap.put(concatObs, counterMap(concatObs) + currentMove)

      XPattern.move(currentMove)
      YPattern.move(currentMove)
    }
  }

//  def bias(): Double = {
//
//  }
  def computeCondEntropy: Double = {
    var result: Double = 0
    cart(List.make(2*obsSize - 2, List[Byte](0, 1))) foreach {
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
