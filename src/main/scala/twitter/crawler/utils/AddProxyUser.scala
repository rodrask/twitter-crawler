package twitter.crawler.utils

import twitter4j.Twitter
import twitter.crawler.common.TwitterService
import twitter.crawler.common.properties
import java.io.FileWriter

object AddProxyUser extends App {
  val clientName: String = args(0)
  val twitter: Twitter = TwitterService.newRestInstance
  twitter.setOAuthConsumer(properties("consumer.key"), properties("consumer.secret"))
  val requestToken = twitter.getOAuthRequestToken
  Console.printf("Url for access: %s", requestToken.getAuthorizationURL)
  val pin = Console.readLine()
  val accessToken = twitter.getOAuthAccessToken(requestToken, pin)

  val file = new FileWriter("accounts.properties", true)
  file.write("%s.access.token=%s\n".format(clientName, accessToken.getToken))
  file.write("%s.access.secret=%s\n".format(clientName, accessToken.getTokenSecret))
  file.close()
}
