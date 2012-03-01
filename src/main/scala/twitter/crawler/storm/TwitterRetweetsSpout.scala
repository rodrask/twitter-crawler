package twitter.crawler.storm

import storm.scala.dsl.StormSpout
import backtype.storm.utils.Utils
import twitter4j.{TwitterException, Tweet, Query, Twitter}
import scala.collection.JavaConversions._
import twitter.crawler.common.TwitterService
import twitter.crawler.common.TwitterService.authorize
import
import twitter.crawler.storages.FutureTasksStorage.getCurrentRtForSearch
import scala.collection.mutable

class TwitterRetweetsSpout extends StormSpout(outputFields = List("tweet")) {
  var twitter: Twitter = _
  setup {
    twitter = TwitterService.restFactory.getInstance()
    authorize(twitter, "search")
  }
  var pendingRT = mutable.Set[Long]()
  var sleepTime = 0

  def nextTuple = {
    if (pendingRT.isEmpty) {
      pendingRT.addAll(getCurrentRtForSearch)
    }
    val twId = pendingRT.headOption
    if (twId.isEmpty){
      Thread sleep 5000
    }

    twId match {
      case None =>
      case Some()
    }
  }

  def search(data: String, c: Int = 1): Option[Seq[Tweet]] = {
    if (c > 3) {
      println("Cannot get query result for " + data)
      None
    }
    val query = new Query(data)
    query.setLang("ru")
    try {
      val result = twitter.search(query)
      Some(result.getTweets)
    }
    catch {
      case ex: TwitterException =>
        if (ex.exceededRateLimitation) {
          println("Rate limit exceed. Sleep until reset")
          Utils.sleep(ex.getRateLimitStatus.getSecondsUntilReset * 1000)
          search(data)
        }
        else {
          println("Catch error: " + ex.getErrorMessage + "\n try one more time")
          Utils.sleep(1000)
          search(data, c + 1)
        }
    }
  }


}
