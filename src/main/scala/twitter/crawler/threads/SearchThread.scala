package twitter.crawler.threads

import twitter.crawler.common.TwitterService
import twitter.crawler.storages.FutureTasksStorage.UrlTask
import scala.collection.JavaConversions._
import twitter4j.{TwitterException, Tweet, QueryResult, Query}
import com.codahale.logula.Logging
import twitter.crawler.storages.{TweetStorage, FutureTasksStorage}
import twitter.crawler.storages.GraphStorage._

object SearchThread extends Thread with Logging {
  val twitter = TwitterService.newRestInstance("search")
  var sleepTime: Long = 1000 * 10

  def buildQuery(task: UrlTask): Query = {
    val query = new Query(task.url)
    query.setRpp(100)
    if (task.lastMessage.isDefined)
      query.setSinceId(task.lastMessage.get)
    query.setResultType(Query.RECENT)
    query
  }

  override def run() = {
    while (true) {
      Thread sleep sleepTime
      (FutureTasksStorage !? 'get_url).asInstanceOf[Option[UrlTask]] match {
        case Some(task) =>
          try {
            log.info("Search for: %s", task.url)
            val result: QueryResult = twitter.search(buildQuery(task))
            val tweets = result.getTweets
            FutureTasksStorage !('put, task.url, result.getMaxId, task.interval)
            tweets foreach {
              status: Tweet =>
                TweetStorage !('index, status)
                saveUrlFromSearch(status.getFromUser, task.url, status.getId, status.getCreatedAt)
            }
          } catch {
            case ex: TwitterException =>
              println(ex.getErrorMessage)
              if (ex.exceededRateLimitation)
                sleepTime = 1000 * 60 * 10l
          }
        case None =>
      }
    }
  }
}