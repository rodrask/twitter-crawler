package twitter.crawler.storm.spouts

import storm.scala.dsl.StormSpout
import java.util.concurrent.LinkedBlockingQueue
import backtype.storm.utils.Utils
import twitter.crawler.common.TwitterService
import twitter4j.{FilterQuery, TwitterStream, Status}
import twitter.crawler.storm.TwitterStreamListener

class TwitterStreamSpout(usersToListen: Seq[Long]) extends StormSpout(outputFields = List("tweet")) with Serializable {
  var twitterStream: TwitterStream = _
  var queue: LinkedBlockingQueue[Status] = _
  val filter: FilterQuery = new FilterQuery(usersToListen.toArray)
  var listener: TwitterStreamListener = _


  setup {
    queue = new LinkedBlockingQueue[Status](1000)
    listener = new TwitterStreamListener(queue)
    val stream = TwitterService.streamFactory.getInstance()
    TwitterService.authorize(stream)
    stream.addListener(listener)
//    stream.filter(filter)
    stream.sample()
  }

  def nextTuple = {
    val status = queue.poll()
    if (status == null)
      Utils.sleep(50)
    else
      emit(status)
  }

  override def close() = {
    twitterStream.cleanUp()
  }
}
