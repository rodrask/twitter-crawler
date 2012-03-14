package twitter.crawler.storm.spouts

import scala.collection.JavaConversions._
import storm.scala.dsl.StormSpout
import twitter.crawler.common.TwitterService
import twitter.crawler.storages.FutureTasksStorage
import twitter.crawler.storages.FutureTasksStorage.RTTask
import twitter4j._

class TwitterRetweetsSpout extends StormSpout(outputFields = List("tweet")) {
  var twitter: Twitter = _
  setup {
    twitter = TwitterService.newRestInstance("search")
  }
  def sleepTime: Long = {
    val now = System.currentTimeMillis()
    if (now > nextCall)
      0l
    else
      nextCall - now
  }
  var currentMessage:Long = 0;
  var nextCall = 0l
  def updateSleepTime(rateLimit: RateLimitStatus)={
    nextCall = System.currentTimeMillis() + 1000 * rateLimit.getSecondsUntilReset / rateLimit.getRemainingHits
  }

  def nextTuple = {
    Thread sleep sleepTime
    (FutureTasksStorage !? 'get_rt).asInstanceOf[Option[RTTask]] match {
      case Some(task) =>
        try{
          val result: ResponseList[Status] = twitter.getRetweets(task.mesageId)
          updateSleepTime(result.getRateLimitStatus)
          result foreach {s: Status => emit(s)}
        } catch {
          case ex: TwitterException =>
            println(ex.getErrorMessage)
            if (ex.exceededRateLimitation)
              nextCall = ex.getRateLimitStatus.getSecondsUntilReset
        }
      case None =>

    } 

  }

}
