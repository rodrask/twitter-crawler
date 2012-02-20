package twitter.crawler.storm

import backtype.storm.tuple.Tuple
import storm.scala.dsl.StormBolt
import twitter4j.Status
import twitter.crawler.storages.StreamStorage._

class TwitterBoltCommon extends StormBolt(outputFields = List()) {
  def execute(t: Tuple) =
    t matchSeq {
      case Seq(status: Status) =>
        saveTweet(status)
        t ack
    }
  override def cleanup():scala.Unit={
    end
  }
}
