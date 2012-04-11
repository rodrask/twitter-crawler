package twitter.crawler
import collection.immutable.SortedSet
package object metrics {
  val SECOND = 1l
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS = Array(DAY, 2 * HOUR, 10 * MINUTE, SECOND)

  def calculateIC(fromHistory: SortedSet[Long], toHistory: SortedSet[Long], fromTs: Long, toTs: Long): (Double, Double) = {
    val fromDistr = new SingleDistribution(fromHistory)
    val toDistr = new SingleDistribution(toHistory)

    fromDistr.computeCounters(fromTs, toTs, INTERVALS)
    toDistr.computeCounters(fromTs, toTs, INTERVALS)

    val directInfluence = new JoinedDistribution(fromHistory, toHistory)
    val reverseInfluence = new JoinedDistribution(toHistory, fromHistory)

    directInfluence.computeCounters(fromTs, toTs, INTERVALS)
    reverseInfluence.computeCounters(fromTs, toTs, INTERVALS)

    return (toDistr.computeCondEntropy - directInfluence.computeCondEntropy, fromDistr.computeCondEntropy - reverseInfluence.computeCondEntropy)
  }
}
