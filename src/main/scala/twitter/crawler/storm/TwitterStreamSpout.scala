package twitter.crawler.storm

import storm.scala.dsl.StormSpout
import java.util.concurrent.LinkedBlockingQueue
import backtype.storm.utils.Utils
import twitter.crawler.common.TwitterService
import twitter4j.{FilterQuery, TwitterStream, Status}

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 09.02.12
 * Time: 23:46
 * To change this template use File | Settings | File Templates.
 */

class TwitterStreamSpout(usersToListen: Seq[Long]) extends StormSpout(outputFields = List("tweet")) with Serializable {
  var twitterStream: TwitterStream = _
  var queue: LinkedBlockingQueue[Status] = _
  val filter: FilterQuery = new FilterQuery(usersToListen.toArray)


  setup{
    queue = new LinkedBlockingQueue[Status](1000)
    val listener = new TwitterStreamListener(queue)
    val stream = TwitterService.streamFactory.getInstance()
    TwitterService.authorize(stream)
    stream.addListener(listener)
    stream.filter(filter)
  }

  def nextTuple = {
    val status = queue.poll()
    if (status == null)
      Utils.sleep(50)
    else
      emit(status)
  }

  override def close()={
    twitterStream.shutdown()
  }
}
