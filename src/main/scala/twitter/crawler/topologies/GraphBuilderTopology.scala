package twitter.crawler.topologies

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, LocalCluster}
import twitter.crawler.storages.{FutureTasksStorage, GraphStorage}
import twitter.crawler.storm.spouts.{TwitterSearchSpout, TwitterRetweetsSpout, TwitterStreamSpout}
import twitter.crawler.storm.bolts.{UrlBolt, RetweetBolt, TwitterBoltCommon}
import twitter.crawler.utils.UrlEnlarger

object GraphBuilderTopology {
  def main(args: Array[String]) = {
    FutureTasksStorage.start()

    val builder = new TopologyBuilder()

    builder.setSpout("tweet", new TwitterStreamSpout(List(1)))
    builder.setSpout("rt_getter", new TwitterRetweetsSpout)
    builder.setSpout("url_getter", new TwitterSearchSpout)

    builder.setBolt("common_twits", new TwitterBoltCommon, 1).shuffleGrouping("tweet")
    builder.setBolt("rt_bolt", new RetweetBolt, 1).shuffleGrouping("rt_getter")
    builder.setBolt("url_bolt", new UrlBolt, 1).shuffleGrouping("url_getter")

    val conf = new Config()
    //conf setDebug true
    val cluster = new LocalCluster()
    cluster.submitTopology("test", conf, builder.createTopology())
    Thread sleep 120*1000
    try {
      cluster.killTopology("test")
      cluster.shutdown()
    }
    catch {
      case ex: Exception =>
        println("Exception again")
    }
    FutureTasksStorage ! 'stop

  }

}