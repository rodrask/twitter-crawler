package twitter.crawler.metrics

import collection.immutable.SortedSet
import collection.mutable
import SingleDistribution.{cart, initMap, log2}
import scala.math.min
import scala.Array.concat


class JoinedDistribution(influencedHistory: SortedSet[Long], selfHistory: SortedSet[Long]) {
  var counterMap: mutable.Map[Array[Byte], Long] = _
  var total: Long = _
  var obsSize: Int = _

  def computeCounters(from: Long, to: Long, intervals: Array[Long]) = {
    obsSize = intervals.size
    val selfPattern = new Pattern(from, intervals)
    val infPattern = new Pattern(from, intervals.take(obsSize - 1))

    counterMap = initMap(obsSize * 2 - 1)
    total = to - selfPattern.rightBorder

    var currentMove: Long = 0
    var concatObs: Array[Byte] = null

    while (selfPattern.rightBorder <= to) {
      concatObs = concat(infPattern.observe(influencedHistory), selfPattern.observe(selfHistory))
      currentMove = min(selfPattern.minMove(selfHistory), infPattern.minMove(influencedHistory))
      counterMap.put(concatObs, counterMap(concatObs) + currentMove)

      selfPattern.move(currentMove)
      infPattern.move(currentMove)
    }
  }

  def computeCondEntropy: Double = {
    var result: Double = 0
    cart(List.make(obsSize - 1, List[Byte](0, 1))) foreach {
      list: List[Byte] =>
        var with_1_Count = 1.0 * counterMap((list.::[Byte](1)).toArray) + 1
        var with_0_Count = 1.0 * counterMap((list.::[Byte](0)).toArray) + 1
        var marginalized = with_1_Count + with_0_Count
        result += (with_1_Count / total) * log2(with_1_Count / marginalized)
        result += (with_0_Count / total) * log2(with_0_Count / marginalized)
    }
    result
  }
}
