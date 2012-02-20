package twitter.crawler.storm

import backtype.storm.tuple.Tuple
import storm.scala.dsl.StormBolt
import twitter4j.Status
import twitter.crawler.storages.streamStorage._

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 12.02.12
 * Time: 16:15
 * To change this template use File | Settings | File Templates.
 */

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
