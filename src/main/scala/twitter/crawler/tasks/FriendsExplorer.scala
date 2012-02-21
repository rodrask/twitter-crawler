package twitter.crawler.tasks

import collection.mutable.ListBuffer

import org.neo4j.graphdb.Node
import twitter.crawler.storages.DBStorage
import twitter.crawler.rest.{RestException, RestRequest, RestActor}
import scala.util.Random
case class FriendsExplorerResult(user: Long, friends: List[Long], usedCalls: Int)

class FriendsExplorer extends Task {
  override var twLimits = 350
  val friendsService = new RestActor[Long, FriendsExplorerResult](
  {
    input: Long =>
      var result = twitter.getFriendsIDs(input, -1)
      var calls = 1
      var data = ListBuffer[Long](result.getIDs: _*)
      while (result.hasNext) {
        result = twitter.getFriendsIDs(input, result.getNextCursor)
        data ++= result.getIDs
        calls += 1
      }
      println("Use " + calls + " calls to twi API for userID: " + input)
      FriendsExplorerResult(input, data.toList, calls)
  }, {
    data: FriendsExplorerResult =>
      DBStorage.storage.insertFriends(data.user, data.friends)
      this ! data
  })

  val rand: Random = new Random(System.currentTimeMillis())
  def getNextUserId: Long = {
    println("getNextUserId")
    val nodes = DBStorage.storage.batchNodesWithoutFriends(100)
    val selectedNode:Node = nodes(rand.nextInt(nodes.size))
    println("Next user: "+selectedNode.getProperty("name", "DUMMY") )
    selectedNode.getProperty("twId").asInstanceOf[Long]
  }

  def act() = {
    link(friendsService)
    loop {
      react {
        case Begin =>
          friendsService.start()
          friendsService ! RestRequest[Long](getNextUserId)
        case Pause =>
          println("Task " + toString + " paused")
          this.wait()
        case Stop =>
          exit()
        case FriendsExplorerResult(user, friends, usedCalls) =>
          twLimits -= usedCalls
          if (twLimits <= 0){
            println("reached task limit")
            exit('RateLimitsForTaskReached)
          }
          else
            friendsService ! RestRequest[Long](getNextUserId)
        case 'RestRateLimitsExceeds =>
          exit('RestRateLimitsExceeds)
        case RestException(ex) =>
          println("get an exception: "+ex)
          twLimits -= 1
          if (twLimits <= 0)
            exit('RateLimitsForTaskReached)
          else
            friendsService ! RestRequest[Long](getNextUserId)


      }
    }
  }
}