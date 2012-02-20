package twitter.crawler.storm

import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.Status
import java.util.concurrent.LinkedBlockingQueue


class TwitterStreamListener(queue: LinkedBlockingQueue[Status]) extends StatusListener {
  override def onStatus(status: Status) = {
    queue.put(status)
  }

  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {
  }

  override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {
    println("Got track limitation notice:" + numberOfLimitedStatuses);
  }

  override def onScrubGeo(userId: Long, upToStatusId: Long) {
  }

  override def onException(ex: Exception) {
    ex.printStackTrace();
  }
}