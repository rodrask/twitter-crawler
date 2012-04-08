package twitter.crawler.threads

import twitter.crawler.common.TwitterService
import twitter.crawler.storages.FutureTasksStorage.RTTask
import scala.collection.JavaConversions._
import twitter4j.{TwitterException, Status, ResponseList, RateLimitStatus}
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.storages.{GraphStorage, FutureTasksStorage}

object RetweetsThread extends Thread {
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
      (FutureTasksStorage !? 'get_rt).asInstanceOf[Option[RTTask]] match {
        case Some(task) =>
          try {
            val result: ResponseList[Status] = twitter.getRetweets(task.mesageId)
            updateSleepTime(result.getRateLimitStatus)
            result foreach {
              status: Status =>
                val rStatus = status.getRetweetedStatus
                GraphStorage ! ('save_rt, status.getUser, rStatus.getUser, status.getId, rStatus.getId, status.getCreatedAt)
            }
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
}
