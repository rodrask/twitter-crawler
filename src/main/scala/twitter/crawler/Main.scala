package twitter.crawler

import metrics.{UrlFeatures, JoinedProcesses, createGraph}
import twitter.crawler.storages.GraphStorage
import collection.immutable.SortedSet
import java.io.FileWriter

object Main extends App {
//  val urls = GraphStorage.getUserUrls("navalny")
//  println(urls)
//  val (n1, ts1) = GraphStorage.getUserUrlsTs("navalny")
//  val (n2, ts2) = GraphStorage.getUserUrlsTs("Anna_Veduta")
//  JoinedProcesses.calculateIT(SortedSet[Long](ts1:_*), SortedSet[Long](ts2:_*))
//  val file = new FileWriter("features.csv")
  val url="http://www.rosagit.info/p/blog-page_24.html"
  val data = GraphStorage.getUrlFactors(url, 0, Long.MaxValue)
  println(new UrlFeatures(url, data).toString)
//  m foreach{
//    case (url, data) =>
//      file.write(new UrlFeatures(url, data).toString)
//      file.write("\n")
//  }
//  createGraph(m)
}
