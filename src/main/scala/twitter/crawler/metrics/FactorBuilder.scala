package twitter.crawler.metrics

import twitter.crawler.storages.UrlRawFactors
import twitter.crawler.metrics.SingleDistribution.log2
import scala.Double
import java.util.{Calendar, Locale}

case class UrlFactors(url: String, uniqueUsers: Int, totalMentions: Int, mentionsEntropy: Double) {
  val en = new Locale("en")
  override def toString() = {
    "\"%s\";%d;%d;%.6f".formatLocal(en,url, totalMentions, uniqueUsers, mentionsEntropy)
  }
}

object FactorBuilder {
  def buildFactors(url: String, raw: UrlRawFactors) = {
    val uniqueUsers = raw.users.size
    val totalMentions = raw.timestamps.size
    val entropy = calculateEntropy(raw.timestamps)
    UrlFactors(url, uniqueUsers, totalMentions, entropy)
  }

  val INTERVAL = 1000 * 60 * 10
  def calculateEntropy(timestamps: List[Long]) = {
    val size: Double = timestamps.size
    var border = timestamps.head + INTERVAL
    var counter = 0
    var result = 0.0
    for (t <- timestamps) {
      if (t <= border)
        counter += 1
      else {
        border += INTERVAL
        var p:Double = counter / size
        result += log2(p) * p
        counter = 1
      }
    }
    -result
  }

  def getHour(ts: Long): Int={
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(ts)
    calendar.get(Calendar.HOUR_OF_DAY)
  }

}
