package twitter.crawler

import metrics.{UrlFeatures, JoinedProcesses, createGraph}
import collection.immutable.SortedSet
import java.io.FileWriter
import io.Source
import storages.{FriendStorage, UrlData, GraphStorage}
import collection.mutable

object Main extends App {
  def saveUrlFeatures = {
    val reader = Source.fromFile("urls.csv")
    val writer = new FileWriter("/home/pokunev/Dropbox/factors.csv")
    var url = ""
    var data: UrlData = null
    var features: UrlFeatures = null
    reader.getLines() foreach {
      line =>
        url = line.split("\t")(0)
        println("calculate for: " + url)
        data = GraphStorage.getUrlFactors(url, 0, Long.MaxValue)
        features = new UrlFeatures(url, data)
        println(features.toString)
        writer.write(features.toCsv)
        writer.write('\n')

    }
    writer.write("end")
    writer.close()
  }

  def id2Names = {
    val reader = Source.fromFile("ids.txt")
    val writer = new FileWriter("/home/pokunev/Dropbox/twitter/names5000.txt")
    var badC = 0
    reader.getLines() foreach {
      line: String =>
        val name = GraphStorage.getUsername(line.toLong)
        if (name != null) {
          writer.write("%s\n".format(name))
          println(name)
        }
        else {
          println("Bad id %s %d".format(line, badC))
          badC += 1
        }
    }
    writer.close()
  }

  val writer = new FileWriter("/home/pokunev/Dropbox/twitter/user-features.csv")

  def save2File(user1: String, user2: String, isFollower: Boolean, rt: Long, it: Double) = {
    writer.write("%s\t%s\t%s\t%s\t%s\n".format(user1, user2, isFollower, rt, it.toString))
  }

  def calculateUsersFeatures() = {
    val reader = Source.fromFile("/home/pokunev/Dropbox/twitter/names5000.txt")
    reader.getLines() foreach {
      name: String =>
        calculateFeaturesForUser(name)
    }
    writer.close()
  }


  def calculateFeaturesForUser(user: String) = {
    val userTs = getTs(user)
    var rt: Long = 0
    var isFollower: Boolean = false
    var retweeterTs: SortedSet[Long] = SortedSet.empty[Long]
    var directIt: Double = 0.0
    val rtResult = GraphStorage.userRT(user)
    val rtUsers = rtResult.map(t => t._1).toSet
    rtResult foreach {
      tuple =>
        val name = tuple._1
        val count = tuple._2
        rt = count
        isFollower = FriendStorage.isRead(name, user)
        retweeterTs = getTs(name)
        if (!userTs.isEmpty && !retweeterTs.isEmpty) {
          directIt = JoinedProcesses.calculateIT(userTs, retweeterTs)
        }
        else {
          directIt = 0.0
        }
        save2File(user, name, isFollower, rt, directIt)
    }

    val followers: Set[String] = FriendStorage.followers(user)
    followers foreach {
      follower =>
        if (!rtUsers.contains(follower)) {
          isFollower = true
          retweeterTs = getTs(follower)
          if (!userTs.isEmpty && !retweeterTs.isEmpty) {
            directIt = JoinedProcesses.calculateIT(userTs, retweeterTs)
          }
          else {
            directIt = 0.0
          }
          rt = GraphStorage.user2userRT(follower, user)
        }
        save2File(user, follower, isFollower, rt, directIt)
    }

  }
  var tsCache: mutable.Map[String, SortedSet[Long]] = mutable.Map()

  def getTs(user: String): SortedSet[Long] = {
    if (!(tsCache contains user)) {
      val (name, ts) = GraphStorage.getUserUrlsTs(user)
      if (ts.size >= 10)
        tsCache += ((user, SortedSet[Long](ts:_*)))
      else
        tsCache += ((user, SortedSet.empty[Long]))
    }

    tsCache(user)
  }
  GraphStorage.dumpPostEgdes(new FileWriter("/home/pokunev/Dropbox/twitter/post-graph.csv"))
}
