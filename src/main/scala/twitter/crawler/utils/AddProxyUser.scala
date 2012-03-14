package twitter.crawler.utils

import twitter4j.Twitter
import twitter.crawler.common.TwitterService
import twitter.crawler.common.commonProperties
import java.io.FileWriter

object AddProxyUser extends App {
  val clientName: String = args(0)
  val twitter: Twitter = TwitterService.anonymousInstance

  val requestToken = twitter.getOAuthRequestToken
  Console.printf("Url for access: %s\n", requestToken.getAuthorizationURL)
  val pin = Console.readLine()
  Console.println("pin "+pin)
  val accessToken = twitter.getOAuthAccessToken(requestToken, pin)
  Console.println("at "+accessToken.getToken+" "+accessToken.getTokenSecret)
  val file = new FileWriter("src/main/resources/accounts.properties", true)
  file.write("%s.access.token=%s\n".format(clientName, accessToken.getToken))
  file.write("%s.access.secret=%s\n".format(clientName, accessToken.getTokenSecret))
  file.close()
}
