package twitter.crawler.storm

import storm.scala.dsl.StormSpout
import backtype.storm.utils.Utils
import twitter4j.{TwitterException, Tweet, Query, Twitter}
import scala.collection.JavaConversions._
import twitter.crawler.common.TwitterService
import twitter.crawler.common.TwitterService.authorize

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 15.02.12
 * Time: 0:00
 * To change this template use File | Settings | File Templates.
 */

class TwitterSearchSpout extends StormSpout(outputFields = List("tweet")) {
  var twitter: Twitter = _

  setup {
    twitter = TwitterService.restFactory.getInstance()
    authorize(twitter)
  }


  def nextTuple = {


  }

  def search(data: String, c:Int=1): Option[Seq[Tweet]] = {
    if (c > 3){
      println("Cannot get query result for "+data)
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
          if (ex.exceededRateLimitation){
            println("Rate limit exceed. Sleep until reset")
            Utils.sleep(ex.getRateLimitStatus.getSecondsUntilReset * 1000)
            search(data)
          }
          else{
            println("Catch error: "+ex.getErrorMessage+"\n try one more time")
            Utils.sleep(1000)
            search(data, c+1)
          }
      }
  }


}
