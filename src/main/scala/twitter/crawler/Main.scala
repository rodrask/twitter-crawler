package twitter.crawler

import twitter.crawler.storages.GraphStorage
import twitter.crawler.metrics.JoinedProcesses
import collection.immutable.SortedSet

object Main extends App {
  val urls = GraphStorage.getUserUrls("navalny")
  println(urls)
  val m = GraphStorage.getUrlSubgraph("http://instagr.am/p/KVvffEoCxg/", 0, Long.MaxValue)
  println(m.keys)
}
