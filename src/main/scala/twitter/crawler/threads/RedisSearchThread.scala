package twitter.crawler.threads

import twitter.crawler.common.TwitterService
import scala.collection.JavaConversions._
import twitter4j.{TwitterException, Tweet, QueryResult, Query}
import com.codahale.logula.Logging
import twitter.crawler.storages.{GraphStorage, RedisFutureStorage, TweetStorage}

object RedisSearchThread extends Thread with Logging {
  var withRemoving: Boolean = false
  val twitter = TwitterService.newRestInstance("search")
  var sleepTime: Long = 1000 * 1

  def buildQuery(url: String, lastMessage: Option[Long]): Query = {
    val query = new Query(url)
    query.setRpp(100)
    query.setLang("ru")
    if (lastMessage.isDefined)
      query.setSinceId(lastMessage.get)
    query.setResultType(Query.RECENT)
    query
  }

  override def run() = {
    while (true) {
      Thread sleep sleepTime
      log.info("Call refinement %d", sleepTime)
      val urlTask = RedisFutureStorage.getUrlTask(withRemoving)
      if (urlTask.isDefined) {
        val (url, lastMessage) = urlTask.get
        log.info("Search for: %s", url)
        try {
          val result: QueryResult = twitter.search(buildQuery(url, lastMessage))
          val tweets = result.getTweets
          if (!withRemoving) {
            RedisFutureStorage.updateLastMessageUrl(url, result.getMaxId, tweets.size())
          }
          tweets foreach {
            status: Tweet =>
              TweetStorage !('index, status)
              GraphStorage !('save_rf_url, status.getFromUser, status.getFromUserId, url, status.getId, status.getCreatedAt)
          }
          sleepTime = 1000 * 1
        } catch {
          case ex: TwitterException =>
            log.error(ex.getErrorMessage)
            if (ex.exceededRateLimitation)
              sleepTime = 1000 * 60 * 10l
        }
      }
    }
  }
}
