package twitter.crawler
import collection.immutable.SortedSet
import scala.math.{min, max}
import twitter.crawler.metrics.SingleDistribution.dumpDistrMap
package object metrics {
  val SECOND = 1l
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS = List(8*HOUR, 2 * HOUR, 10 * MINUTE, SECOND)
  val INTERVALS_TEST = List(60 * MINUTE, SECOND)

  def calculateIC(YHist: SortedSet[Long], XHist: SortedSet[Long], fromTs: Long, toTs: Long): (Double, Double) = {
    println(YHist.size)
    println(XHist.size)
    val YDistr = new SingleDistribution(YHist)
    val XDistr = new SingleDistribution(XHist)

    YDistr.computeCounters(fromTs, toTs, INTERVALS_TEST)
    XDistr.computeCounters(fromTs, toTs, INTERVALS_TEST)

    val Y_X_Influence = new JoinedDistribution(YHist, XHist)
    val X_Y_Influence = new JoinedDistribution(XHist, YHist)

    Y_X_Influence.computeCounters(fromTs, toTs, INTERVALS_TEST)
    X_Y_Influence.computeCounters(fromTs, toTs, INTERVALS_TEST)

    val XEntr = XDistr.computeCondEntropy
    dumpDistrMap(XDistr.counterMap)
    println("Xfuture | Xpast entropy: "+XEntr)

    val directTransfer = Y_X_Influence.computeCondEntropy
    dumpDistrMap(Y_X_Influence.counterMap)
    println("Xfuture | Xpast,Ypast entropy: "+directTransfer)

    val YEntr = YDistr.computeCondEntropy
    dumpDistrMap(YDistr.counterMap)
    println("Yfuture | Ypast entropy "+YEntr)

    val reverseTransfer = X_Y_Influence.computeCondEntropy
    dumpDistrMap(X_Y_Influence.counterMap)
    println("Yfuture |Xpast, Ypast entropy: "+reverseTransfer)

    return (XEntr - directTransfer, YEntr - reverseTransfer)
  }
  def calculateIC(fromHistory: SortedSet[Long], toHistory: SortedSet[Long]): (Double, Double) = {
    val (from, to) = borders(fromHistory, toHistory)
    calculateIC(fromHistory: SortedSet[Long], toHistory: SortedSet[Long], from: Long, to: Long)
  }

  def borders(fromHistory: SortedSet[Long], toHistory: SortedSet[Long]): (Long, Long)={
    (max(fromHistory.min, toHistory.min), min(fromHistory.max, toHistory.max))
  }

}
