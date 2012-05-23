package twitter.crawler

import twitter.crawler.storages.GraphStorage
import twitter.crawler.metrics.JoinedProcesses
import twitter.crawler.metrics.createGraph
import collection.immutable.SortedSet

object Main extends App {
  //val urls = GraphStorage.getUserUrls("navalny")
  //println(urls)
//  val (n1, ts1) = GraphStorage.getUserUrlsTs("navalny")
//  val (n2, ts2) = GraphStorage.getUserUrlsTs("Anna_Veduta")
//  JoinedProcesses.calculateIT(SortedSet[Long](ts1:_*), SortedSet[Long](ts2:_*))

  val m = GraphStorage.dumpUrlFactors(0, Long.MaxValue, 0, 10)
  m foreach(f => println(f))
//  createGraph(m)
}
