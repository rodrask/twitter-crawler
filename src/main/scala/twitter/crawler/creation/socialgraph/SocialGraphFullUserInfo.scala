package twitter.crawler.creation.socialgraph

import twitter.crawler.common._
import org.neo4j.graphdb.Node
import twitter4j.{TwitterException, User, ResponseList, Twitter}
import collection.mutable.ListBuffer
import twitter.crawler.storages.{GraphStorage}

object SocialGraphFullUserInfo extends App {
  Console.println("Start process crawling full user info...")
  val usernames = loadNames("src\\main\\resources\\twi_top100.txt").toArray
  var twitterRest: Twitter = TwitterService.restFactory.getInstance()
  TwitterService.authorize(twitterRest)
  while (true) {
    try {
      val nodes: List[Node] = GraphStorage.getUsersNotIndex(100)
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

    }
    catch {
      case e: TwitterException =>
        if (e.exceededRateLimitation()) {
          var sleepSeconds: Long = 30 * 60
          Thread.sleep(sleepSeconds * 1000)
        } else {
          Thread.sleep(10 * 1000)
        }
      case e: Exception =>
        Console.println("Some excepton...sleep on 10 seconds")
        Thread.sleep(10 * 1000)
    }
  }

  Console.println("End process crawling full user info...")
}
