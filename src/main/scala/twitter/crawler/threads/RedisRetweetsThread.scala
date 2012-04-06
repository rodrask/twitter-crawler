package twitter.crawler.threads

import twitter.crawler.common.TwitterService
import scala.collection.JavaConversions._
import twitter4j.{TwitterException, Status, ResponseList, RateLimitStatus}
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.storages.{RedisFutureStorage, FutureTasksStorage}

object RedisRetweetsThread extends Thread {
  val twitter = TwitterService.newRestInstance("search")

  def sleepTime: Long = {
    val now = System.currentTimeMillis()
    if (now > nextCall)
      0l
    else
      nextCall - now
  }

  var currentMessage: Long = 0;
  var nextCall = 0l

  def updateSleepTime(rateLimit: RateLimitStatus) = {
    nextCall = System.currentTimeMillis() + 1000 * rateLimit.getSecondsUntilReset / rateLimit.getRemainingHits
  }
  override def run() = {
    while (true) {
      Thread sleep sleepTime
      val messageId = RedisFutureStorage.getRTTask()
      if (messageId.isDefined){
        try {
          val result: ResponseList[Status] = twitter.getRetweets(messageId.get)
          updateSleepTime(result.getRateLimitStatus)
          result foreach {
            status: Status =>
              val rStatus = status.getRetweetedStatus
              saveRetweet(status.getUser, rStatus.getUser, status.getId, rStatus.getId, status.getCreatedAt)
          }
        } catch {
          case ex: TwitterException =>
            println(ex.getErrorMessage)
            if (ex.exceededRateLimitation)
              nextCall = ex.getRateLimitStatus.getSecondsUntilReset
        }
      }
    }
  }
}
