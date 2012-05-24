package twitter.crawler
import collection.immutable.SortedSet
import scala.math.{min, max}
import scala.math.log

package object metrics {
  val SECOND = 1l
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS = List(12*HOUR, 2 * HOUR, 10 * MINUTE, SECOND)
  val ADDITIONAL_INTERVALS = List(12*HOUR, 2 * HOUR, 10 * MINUTE)

  val INTERVALS_TEST = List(60 * MINUTE, SECOND)

  def nodePairs[T](map: Map[T, Any]): Iterator[IndexedSeq[T]]= map.keys.toIndexedSeq.combinations(2)

  val LN2 = log(2)
  def log2(x: Double) = {
    log(x) / LN2
  }

  def createGraph(mapData: Map[String, SortedSet[Long]])={
    nodePairs(mapData.filter(entry => entry._2.size >= 10 )).foreach{
      pair:IndexedSeq[String] =>
        val first = pair(0)
        val second = pair(1)
        val direct = JoinedProcesses.calculateIT(mapData(pair(0)), mapData(pair(1)))
        val reverse = JoinedProcesses.calculateIT(mapData(pair(1)), mapData(pair(0)))
    }
    null
  }


}
