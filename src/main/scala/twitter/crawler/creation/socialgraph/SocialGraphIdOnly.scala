package twitter.crawler.creation.socialgraph

import org.neo4j.graphdb.Node
import twitter4j.{TwitterException, User, ResponseList, Twitter}
import scala.util.Random
import collection.mutable.ListBuffer
import twitter.crawler.common._
import twitter.crawler.storages.GraphStorage
import scala.util.control.Breaks._

object SocialGraphIdOnly extends App {
  Console.println("Start process create social graph by id user...")
  val minCreationUser: Int = 1000000
  val usernames = loadNames("src\\main\\resources\\twi_top100.txt").toArray
  var twitterRest: Twitter = TwitterService.restFactory.getInstance()
  TwitterService.authorize(twitterRest)
  val users: ResponseList[User] = twitterRest.lookupUsers(usernames)
  var id = ListBuffer[Long]()
  var position: Int = 0
  while (position < users.size()) {
    id += users.get(position).getId
    position += 1
  }
  var countUser: Int = 0
  countUser = GraphStorage.saveFriendship(1, id.toSeq, countUser)
  val rand: Random = new Random(System.currentTimeMillis())
  breakable {
    while (true) {
      if (countUser > minCreationUser) {
        break;
      }
      val nodes: List[Node] = GraphStorage.batchNodesWithoutFriends(100)
      val node: Node = nodes(rand.nextInt(nodes.size))
      try {
        val user_id = node.getProperty("twId").asInstanceOf[Long]
        var friendsIDs = twitterRest.getFriendsIDs(user_id, -1)
        var ids = ListBuffer[Long](friendsIDs.getIDs: _*)
        while (friendsIDs.hasNext) {
          friendsIDs = twitterRest.getFriendsIDs(user_id, friendsIDs.getNextCursor)
          ids ++= friendsIDs.getIDs
        }
        countUser = GraphStorage.saveFriendship(user_id, ids.toSeq, countUser)
      }
      catch {
        case e: TwitterException =>
          if (e.exceededRateLimitation()) {
            var sleepSeconds: Long = 30 * 60
            Thread.sleep(sleepSeconds * 1000)
          } else {
            Console.println("Twitter excepton..." + e.getMessage + "...sleep on 10 seconds")
            Thread.sleep(10 * 1000)
          }
        case e: Exception =>
          Console.println("Some excepton..." + e.getMessage + "...sleep on 10 seconds")
          Thread.sleep(10 * 1000)
      }
    }
  }
  Console.println("End process create social graph by id user...")
}