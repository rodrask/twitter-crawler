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

  def calculateUsersFeatures() = {
    val reader = Source.fromFile("/home/pokunev/Dropbox/twitter/names5000.txt")
    //    reader.getLines() foreach {
    //      line: String =>
    //        val
  }

  def calcForUser(user: String) = {
    val userTs = getTs(user)
    var rt: Long = _
    var friend: Boolean = _
    var follower: Boolean = _
    var retweeterTs: SortedSet[Long] = _
    var directIt: Double = _
    var reverseIt: Double = _
    GraphStorage.userRT(user) foreach {
      case (name, count) =>
        rt = count
        friend = FriendStorage.isRead(user, name)
        follower = FriendStorage.isRead(name, user)
        retweeterTs = getTs(name)
        if (!userTs.isEmpty && !retweeterTs.isEmpty) {
            directIt = JoinedProcesses.calculateIT(userTs, retweeterTs)
            reverseIt = JoinedProcesses.calculateIT(retweeterTs, userTs)
        }
    }
    println("%s %s %d %d %d ")
  }

  var tsCache: mutable.Map[String, SortedSet[Long]] = mutable.Map()

  def getTs(user: String): SortedSet[Long] = {
    if (!(tsCache contains user)) {
      val ts = GraphStorage.userUrlsTs(user)
      if (ts.size >= 10)
        tsCache += ((user, ts))
      else
        tsCache += ((user, SortedSet.empty[Long]))
    }
    tsCache(user)
  }

  GraphStorage.userRT("navalny") foreach {
    tuple =>
      println(tuple)
  }
}
