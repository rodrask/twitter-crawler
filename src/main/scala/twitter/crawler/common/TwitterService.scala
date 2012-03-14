package twitter.crawler.common

import twitter4j.auth.{OAuthSupport, AccessToken}
import scala.collection.mutable.Map
import twitter4j.{Twitter, TwitterFactory, TwitterStreamFactory}

object TwitterService {
  val accountProperties = loadConf("src/main/resources/accounts.properties")
  val access_token = "access.token"
  val access_secret = "access.secret"

  val streamFactory = new TwitterStreamFactory()
  val restFactory = new TwitterFactory();

  def newRestInstance(prefix: String=""): Twitter =
  {
    val result = restFactory.getInstance
    authorize(result, prefix)
    result
  }

  def anonymousInstance = {
    val result = restFactory.getInstance
    setConsumer(result)
    result
  }

  var defaultTwitter = newRestInstance()

  var defaultStream = streamFactory.getInstance()
  authorize(defaultStream)

  def setConsumer(twitter: OAuthSupport) = twitter.setOAuthConsumer(commonProperties("consumer.key"), commonProperties("consumer.secret"))
  def createAccess(prefix: String = ""): AccessToken =
  {
    new AccessToken(accountProperties(prefix + access_token), accountProperties(prefix + access_secret))
  }

  def authorize(twitter: OAuthSupport, prefix: String = ""): Unit = {
    setConsumer(twitter)
    val pprefix = if (prefix.length() == 0) prefix else prefix+"."
    twitter.setOAuthAccessToken(createAccess(pprefix))
  }
}