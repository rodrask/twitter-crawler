package twitter.crawler.rest

import twitter.crawler.common._
import twitter4j.{RateLimitStatus, Twitter}

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