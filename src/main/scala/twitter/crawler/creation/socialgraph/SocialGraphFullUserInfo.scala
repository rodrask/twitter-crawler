package twitter.crawler.creation.socialgraph

import twitter.crawler.common._
import twitter4j.{TwitterException, User, ResponseList, Twitter}
import collection.mutable.ListBuffer
import twitter.crawler.storages.{GraphStorage}
import scala.util.control.Breaks._
import twitter4j.internal.http.HttpResponseCode

object SocialGraphFullUserInfo extends App {
  Console.println("Start process crawling full user info...")
  var twitterRest: Twitter = TwitterService.restFactory.getInstance()
  TwitterService.authorize(twitterRest)
  var countUserId = 100;
  breakable {
    while (true) {
      var nodes = GraphStorage.getUsersNotIndex(countUserId)
      try {
        if (nodes.isEmpty) {
          break;
        }
        var ids = ListBuffer[Long]()
        nodes foreach {
          node => ids += node.getProperty("twId").toString.toLong
        }
        var users: ResponseList[User] = twitterRest.lookupUsers(ids.toArray[Long])
        var position: Int = 0
        while (position < users.size()) {
          GraphStorage.indexUserInfo(users.get(position).asInstanceOf[User])
          position += 1
        }
        countUserId = 100
      }
      catch {
        case e: TwitterException =>
          if (e.exceededRateLimitation()) {
            var sleepSeconds: Long = 30 * 60
            Thread.sleep(sleepSeconds * 1000)
          } else {
            Console.println("Twitter excepton..." + e.getMessage + "...sleep on 10 seconds")
            if (e.getStatusCode == HttpResponseCode.NOT_FOUND) {
              if (countUserId == 1) {
                GraphStorage.indexUserInfo(nodes)
              } else {
                countUserId = 1
              }
            }
            Thread.sleep(10 * 1000)
          }
        case e: Exception =>
          Console.println("Some excepton..." + e.getMessage + "...sleep on 10 seconds")
          Thread.sleep(10 * 1000)
      }
    }
  }
  Console.println("End process crawling full user info...")
}
