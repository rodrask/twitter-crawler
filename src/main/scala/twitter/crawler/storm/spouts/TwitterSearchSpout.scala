package twitter.crawler.storm.spouts

import storm.scala.dsl.StormSpout
import scala.collection.JavaConversions._
import twitter.crawler.common.TwitterService
import twitter.crawler.storages.FutureTasksStorage
import twitter4j._
import twitter.crawler.storages.FutureTasksStorage.UrlTask

class TwitterSearchSpout extends StormSpout(outputFields = List("url", "tweet")) {
  var twitter: Twitter = _
  setup {
    twitter = TwitterService.newRestInstance("search")
  }
  var sleepTime: Long = 1000*10

  def buildQuery(task: UrlTask): Query={
    val query = new Query(task.url)
    query.setRpp(100)
    if (task.lastMessage.isDefined)
      query.setSinceId(task.lastMessage.get)
    query.setResultType(Query.RECENT)
    query
  }

  def nextTuple = {
    Thread sleep sleepTime
    (FutureTasksStorage !? 'get_url).asInstanceOf[Option[UrlTask]] match {
      case Some(task) =>
        try{
          println("Search for: "+task.url)
          val result: QueryResult = twitter.search(buildQuery(task))
          val tweets = result.getTweets
          FutureTasksStorage ! ('put, task.url, result.getMaxId, task.interval)
          tweets foreach {t: Tweet => emit(task.url, t)}
        } catch {
          case ex: TwitterException =>
            println(ex.getErrorMessage)
            if (ex.exceededRateLimitation)
              sleepTime = 1000*60*10l
        }
      case None =>
    }
  }
}
