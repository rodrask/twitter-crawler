package twitter.crawler.storm

import storm.scala.dsl.StormBolt
import backtype.storm.tuple.Tuple
import twitter4j.Status
import java.util.Date
import twitter.crawler.storages.StreamStorage.saveUrlRecord
import twitter.crawler.common.extractURL

case class UrlRecord(twId: Long, userId: Long, url: String, timestamp: Date)

class TwitterUrlFilter() extends StormBolt(outputFields = List("user", "url")) {
  def execute(t: Tuple) = t matchSeq {
    case Seq(status: Status) =>
      val userId = status.getUser.getId
      val twiId = status.getId
      val ts = status.getCreatedAt
      status.getURLEntities foreach {ue =>
        if (ue != null)
          saveUrlRecord(twiId, userId, ts, extractURL(ue))
      }
      t ack
  }

}
