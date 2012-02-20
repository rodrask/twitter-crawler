package twitter.crawler.common

import twitter4j.{TwitterFactory, TwitterStreamFactory}
import twitter4j.auth.{OAuthSupport, AccessToken}
import scala.collection.mutable.Map

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 31.01.12
 * Time: 3:12
 * To change this template use File | Settings | File Templates.
 */

object TwitterService {
  val streamFactory = new TwitterStreamFactory()
  val restFactory = new TwitterFactory();

  def accessToken(properties: Map[String, String]=properties) = new AccessToken(properties("access.token"), properties("access.secret"))

  var twitter = restFactory.getInstance()
  authorize(twitter)

  var stream = streamFactory.getInstance()
  authorize(stream)

  def authorize(twitter: OAuthSupport, properties: Map[String, String]=properties): Unit = {
    twitter.setOAuthConsumer(properties("consumer.key"), properties("consumer.secret"))
    twitter.setOAuthAccessToken(accessToken())
  }
}