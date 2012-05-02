package twitter.crawler.metrics

import scala.collection.mutable
import twitter.crawler.storages.GraphStorage.getUserUrlsTs
import collection.immutable.SortedSet
import scala.Predef._
import twitter.crawler.metrics.SingleDistribution.computeCondEntropy
import java.io.FileWriter

class SubgraphBuilder(names: Seq[String]) {

  val conditionalEntropy: mutable.Map[String, Double] = mutable.Map()
  val tsMap: mutable.Map[String, SortedSet[Long]] = mutable.Map()

  def fillMap() = {
    names foreach {
      name =>
        val currentTs = System.currentTimeMillis()
        val timestamps = SortedSet(getUserUrlsTs(name, 0l, currentTs)._2: _*)
        if (timestamps.size >= 10) {
          println("save "+name)
          tsMap(name) = timestamps
          val distr = new SingleDistribution(timestamps.head, timestamps.last + 1, timestamps)
          conditionalEntropy(name) = computeCondEntropy(distr.computeCounters(INTERVALS), distr.total)
        }
    }
  }

  def computeEdges() = {
    val ITFile = new FileWriter("it_n.txt", false)
    tsMap.keys foreach {
      k1 =>
        val ts1 = tsMap(k1)
        tsMap.keys foreach {
          k2 =>
            println("user "+k2)
            val ts2 = tsMap(k2)
            println("ts "+ts2)
            val (left, right) = borders(ts1, ts2)
            val jd = new JoinedDistribution(left , right, ts2.range(left, right+1), ts1.range(left, right+1))
            val entr = computeCondEntropy(jd.computeCounters(INTERVALS), jd.total)
            println("joined entropy "+entr)
            val it = conditionalEntropy(k1) - entr
            println("save edge from "+k2+" to "+k1)
            ITFile.write("%s, %s, %s\n".format(k2, k1, it))
        }
    }
    ITFile.close()

  }
}
