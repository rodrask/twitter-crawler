package twitter.crawler

import metrics.{SubgraphBuilder, FactorBuilder, calculateIC}
import twitter.crawler.storages.GraphStorage.{getUserUrlsTs, userUrlsTs, getUsername, userRetweeters, getUrlTimestamps, getUrlFactors}
import scala.Predef._
import twitter.crawler.common.{loadId,loadTwiNames}
import java.io.FileWriter

object Main extends App {
  val ids = loadId("ids.txt")
  val ITFile = new FileWriter("it.txt", false)

  def saveIC(extUser: String, intUser: String, itScore: Double) = {
    println("saving " + itScore)
    ITFile.write("%s, %s, %s\n".format(extUser, intUser, itScore.toString))
  }

  def calcITForUsers(uIds: Seq[Long]) = {
    ids foreach {
      id: Long =>
        var extUrls = userUrlsTs(id)
        if (extUrls.size > 10) {
          var extName = getUsername(id)
          var followers = userRetweeters(id)
          println("retweeters: " + followers)
          followers foreach {
            followerId: Long =>
              var intUrls = userUrlsTs(followerId)
              if (intUrls.size > 10) {
                var intName = getUsername(followerId)
                println("Calc IT for " + extName + " to " + intName)
                var IC = calculateIC(extUrls, intUrls)
                saveIC(extName, intName, IC)
              }
          }
        }
    }
    ITFile.close()
  }

  val sb = new SubgraphBuilder(loadTwiNames("names.txt"))
  sb.fillMap()
  sb.computeEdges()
}
