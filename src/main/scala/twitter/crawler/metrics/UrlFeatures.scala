package twitter.crawler.metrics

import scala.collection.immutable.Map
import collection.immutable.SortedSet
import twitter.crawler.metrics.{Distribution, HOUR}
import org.neo4j.graphdb.Node
import collection.SortedMap
import twitter.crawler.storages.{GraphStorage, UrlData}

class UrlFeatures(url: String, data: UrlData) {
  val featuresMap = calculate()

  def calculate(): SortedMap[String, AnyVal] = {
    val users_Hour_History = GraphStorage.getUrlSubgraph(url, 0, (data.timestamps.head + HOUR) * 1000)
    val users_12Hour_History = GraphStorage.getUrlSubgraph(url, 0, (data.timestamps.head + 12 * HOUR) * 1000)
    SortedMap[String, AnyVal](
      UrlFeatures.featuresFromTs(data.timestamps) ++
        UrlFeatures.userFeatures(users_Hour_History, "h_") ++
        UrlFeatures.userFeatures(users_12Hour_History, "d_")
        : _*)

  }

  override def toString = {
    val buffer = new StringBuilder(url + "\t")
    featuresMap foreach {
      case (key, value) =>
        buffer append "%s:%s\t".format(key, value)
    }
    buffer.toString()
  }
}

object UrlFeatures {
  def featuresFromTs(urlTs: SortedSet[Long]): Seq[(String, AnyVal)] = {
    val distribution = new Distribution[Long](Distribution.diffStream(urlTs))
    //val cumulDiff = urlTs map (t => t - urlTs.head)
    val firstHour = urlTs.to(urlTs.head + HOUR)
    val fDistr: Distribution[Long] = new Distribution[Long](Distribution.diffStream(firstHour))
    var result: Map[String, AnyVal] = Map()
    List(("count", urlTs.size), ("hourCount", firstHour.size), ("entropy", distribution.entropy()), ("hourEntropy", fDistr.entropy()))
  }

  def nodePairs[T](map: Map[T, Any]): Iterator[IndexedSeq[T]] = map.keys.toIndexedSeq.combinations(2)

  def userFeatures(usersHistory: Map[String, SortedSet[Long]], prefix: String = ""): Seq[(String, AnyVal)] = {
    var totalIT = 0.0
    var edges = 0
    nodePairs[String](usersHistory.filter(entry => entry._2.size >= 10)).foreach {
      pair: IndexedSeq[String] =>
        val first = pair(0)
        val second = pair(1)
        val direct = JoinedProcesses.calculateIT(usersHistory(first), usersHistory(second))
        val reverse = JoinedProcesses.calculateIT(usersHistory(second), usersHistory(first))
        edges += 1
        totalIT += direct
        totalIT += reverse
    }
    List((prefix + "density", totalIT / edges), (prefix+"users", usersHistory.keys.size))
  }
}
