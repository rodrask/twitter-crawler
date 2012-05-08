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
          tsMap(name) = timestamps
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
            val ts2 = tsMap(k2)
            val (left, right) = borders(ts1, ts2)

            val distr = new SingleDistribution(left, right, ts1.range(left, right+1))
            val cE1 = computeCondEntropy(distr.computeCounters(INTERVALS), distr.total)

            val jd = new JoinedDistribution(left , right, ts2.range(left, right+1), ts1.range(left, right+1))
            val entr = computeCondEntropy(jd.computeCounters(INTERVALS), jd.total)
            println("joined entropy "+k2+" to"+k1+" "+entr)
            val it = cE1 - entr

            println("save edge from "+k2+" to "+k1+" "+it)
            ITFile.write("%s, %s, %s\n".format(k2, k1, it))
        }
    }
    ITFile.close()

  }
}
