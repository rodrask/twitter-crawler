package twitter.crawler

import twitter.crawler.metrics.SingleDistribution.{dumpDistrMap, computeCondEntropy}
import metrics.{SingleDistribution, calculateIC, INTERVALS_TEST}
import twitter.crawler.storages.GraphStorage.{userUrlsTs, getUsername, userRetweeters}
import twitter.crawler.common.loadId
import twitter.crawler.storages.FriendStorage.userFollowers
import collection.immutable.SortedSet
import java.io.FileWriter

object Main extends App {
  val ids = loadId("ids.txt")
  val ITFile = new FileWriter("it.txt", false)
  def saveIC(extUser: String, intUser: String, itScore: Double)={
    println("saving "+itScore)
    ITFile.write("%s, %s, %s\n".format(extUser, intUser, itScore.toString))
  }
  ids foreach {
    id:Long =>
      var extUrls = userUrlsTs(id)
      if (extUrls.size > 10){
        var extName = getUsername(id)
        var followers = userRetweeters(id)
        println("retweeters: "+followers)
        followers foreach{
          followerId:Long =>
            var intUrls = userUrlsTs(followerId)
            if (intUrls.size > 10){
              var intName = getUsername(followerId)
              println("Calc IT for "+extName+" to "+intName)
              var IC = calculateIC(extUrls, intUrls)
              saveIC(extName, intName, IC)
            }
        }
      }
  }
  ITFile.close()
//  val Yid = 82299300l
//  val Xid = 381163852l
//  val YHist = userUrlsTs(Yid)
//  val Xhist = userUrlsTs(Xid)
//  println("---------------------")
//  println(calculateIC(YHist, Xhist))
//  println("---------------------")
//  println(calculateIC(Xhist, YHist))
//  val from = 1333662624
//  val to = 1334077574
//  val yD = new SingleDistribution(from, to, YHist.range(from, to+1))
//  dumpDistrMap(yD.computeCounters(INTERVALS_TEST))

}
