package twitter.crawler.rest

import twitter.crawler.common._
import twitter4j.{RateLimitStatus, Twitter}


/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 26.01.12
 * Time: 9:04
 * To change this template use File | Settings | File Templates.
 */

object RestActorsRegistry {
  val rateLimitLoorker = new RestActor[Int, RateLimitStatus](
  {
    i: Int =>
      TwitterService.twitter.getRateLimitStatus
  }, {
    status: RateLimitStatus =>
    println("There are "+status.getRemainingHits+" calls. Reset in "+status.getSecondsUntilReset / 60.0 + " seconds")
  }
  )
}