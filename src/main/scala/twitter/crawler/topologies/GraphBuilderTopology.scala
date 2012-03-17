package twitter.crawler.topologies

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, LocalCluster}
import twitter.crawler.storm.bolts.{UrlBolt, RetweetBolt, TwitterBoltCommon}
import twitter.crawler.utils.UrlEnlarger
import twitter.crawler.storages.{TweetStorage, FutureTasksStorage, GraphStorage}
import twitter.crawler.storm.spouts._
import org.neo4j.cypher.symbols.ScalarType
import twitter4j.{TwitterStreamFactory, TwitterStream}
import twitter.crawler.storm.TwitterStreamListener
import twitter.crawler.common.TwitterService
import twitter.crawler.threads.{SearchThread, RetweetsThread}

object GraphBuilderTopology {
  def main(args: Array[String]) = {
    TweetStorage.start()
    FutureTasksStorage.start()
    RetweetsThread.start()
    SearchThread.start()
    val tStream = TwitterService.newStreamInstance()
    tStream.addListener(new TwitterStreamListener)
    tStream.sample()
    
    Thread sleep  60*1000

    RetweetsThread.interrupt
    SearchThread.interrupt()
    tStream.shutdown()

    FutureTasksStorage ! 'stop
    TweetStorage ! 'stop
  }

}