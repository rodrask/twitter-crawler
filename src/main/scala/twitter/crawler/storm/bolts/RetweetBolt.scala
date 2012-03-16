package twitter.crawler.storm.bolts

import backtype.storm.tuple.Tuple
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.storages.TweetStorage
import storm.scala.dsl.StormBolt
import twitter4j.{Tweet, Status}

class RetweetBolt  extends StormBolt(outputFields = List()) {
  def execute(t: Tuple) =
    t matchSeq {
      case Seq(status: Status) =>
        val rStatus = status.getRetweetedStatus
        saveRetweet(status.getUser, rStatus.getUser, status.getId, rStatus.getId, status.getCreatedAt)
    }
}

class UrlBolt extends StormBolt(outputFields = List()){
  def execute(t: Tuple) =
    t matchSeq {
      case Seq(url:String, status: Tweet) =>
        TweetStorage ! ('index, status)
        saveUrlFromSearch(status.getFromUser, url, status.getId, status.getCreatedAt)
    }
}

