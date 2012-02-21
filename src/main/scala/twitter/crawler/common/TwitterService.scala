package twitter.crawler.common

import twitter4j.{TwitterFactory, TwitterStreamFactory}
import twitter4j.auth.{OAuthSupport, AccessToken}
import scala.collection.mutable.Map

object TwitterService {
  val streamFactory = new TwitterStreamFactory()
  val restFactory = new TwitterFactory();

  def newRestInstance = restFactory.getInstance

  def accessToken(properties: Map[String, String]=properties): Option[AccessToken] = 
  {
    if ((properties contains "access.token") && (properties contains "access.secret"))
      Some(new AccessToken(properties("access.token"), properties("access.secret")))
    else
      None
  }

  var twitter = restFactory.getInstance()
  authorize(twitter)

  var stream = streamFactory.getInstance()
  authorize(stream)

  def authorize(twitter: OAuthSupport, properties: Map[String, String]=properties): Unit = {
    twitter.setOAuthConsumer(properties("consumer.key"), properties("consumer.secret"))
    accessToken() match {
      case None => return;
      case Some(token) => twitter.setOAuthAccessToken(token)
    }
  }
}