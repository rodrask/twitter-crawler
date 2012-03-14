package twitter.crawler.storm.spouts

import storm.scala.dsl.StormSpout
import twitter4j.Twitter
import twitter.crawler.common.TwitterService

class TwitterRetweetsSpout extends StormSpout(outputFields = List("tweet")) {
  var twitter: Twitter = _
  setup {
    twitter = TwitterService.newRestInstance("rt")
  }
  var sleepTime = 10*1000

  def nextTuple = {
  }

}
