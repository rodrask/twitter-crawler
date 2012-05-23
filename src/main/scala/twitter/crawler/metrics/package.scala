package twitter.crawler
import collection.immutable.SortedSet
import scala.math.{min, max}
import twitter.crawler.metrics.SingleDistribution.{dumpDistrMap, computeCondEntropy}
package object metrics {
  val SECOND = 1l
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS = List(12*HOUR, 2 * HOUR, 10 * MINUTE, SECOND)
  val ADDITIONAL_INTERVALS = List(12*HOUR, 2 * HOUR, 10 * MINUTE)
  val INTERVALS_TEST = List(60 * MINUTE, SECOND)

  def calculateIC(YHist: SortedSet[Long], XHist: SortedSet[Long], fromTs: Long, toTs: Long): Double = {
    val XDistr = new SingleDistribution(fromTs, toTs,XHist.range(fromTs, toTs+1))
    val Y_X_Distr = new JoinedDistribution(fromTs, toTs, YHist.range(fromTs, toTs+1), XHist.range(fromTs, toTs+1))
    val xCounters = XDistr.computeCounters(INTERVALS)
    val XEntr = computeCondEntropy(xCounters, XDistr.total)

    val yxCounters = Y_X_Distr.computeCounters(INTERVALS)
    val YXEntr = computeCondEntropy(yxCounters, Y_X_Distr.total)

    return XEntr - YXEntr
  }
  def calculateIC(fromHistory: SortedSet[Long], toHistory: SortedSet[Long]): Double = {
    val (from, to) = borders(fromHistory, toHistory)
    calculateIC(fromHistory: SortedSet[Long], toHistory: SortedSet[Long], from: Long, to: Long)
  }

  def borders(fromHistory: SortedSet[Long], toHistory: SortedSet[Long]): (Long, Long)={
    (max(fromHistory.min, toHistory.min), min(fromHistory.max, toHistory.max))
  }

}
