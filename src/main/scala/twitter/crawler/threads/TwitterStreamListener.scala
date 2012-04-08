package twitter.crawler.storm

import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.Status
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.common._
import twitter.crawler.utils.RedisUrlEnlarger._
import com.codahale.logula.Logging
import twitter.crawler.storages.{GraphStorage, RedisFutureStorage, TweetStorage}


class TwitterStreamListener extends StatusListener with Logging {
  def performRetweet(status: Status): Unit = {
    if (status.isRetweet) {
      val rStatus = status.getRetweetedStatus
      TweetStorage ! ('index, rStatus)
      GraphStorage ! ('save_rt, status.getUser, rStatus.getUser, status.getId, rStatus.getId, status.getCreatedAt)
      RedisFutureStorage.putRTTask(rStatus.getId)
    }
  }

  def performMentions(status: Status): Unit = {
    val mentions = status.getUserMentionEntities
    if (mentions == null)
      return
    val id = status.getId
    mentions foreach {
      mention =>
        GraphStorage ! ('save_mention, status.getUser, mention.getId, mention.getScreenName, id, status.getCreatedAt)
    }
  }

  def performUrls(status: Status): Unit = {
    val urls = status.getURLEntities
    if (urls == null)
      return
    urls foreach {
      url =>
        val extractedUrl = extractURL(url)
        var enlargedUrl: String = null
        try {
          enlargedUrl = enlarge(extractedUrl)
        }
        catch {
          case ex: Exception =>
            log.error("Exception when enlarging url: %s",ex.getMessage)
            enlargedUrl = extractedUrl
        }
        GraphStorage ! ('save_url, status.getUser, extractedUrl, enlargedUrl, status.getId, status.getCreatedAt)
        RedisFutureStorage.putUrlTask(enlargedUrl)
    }
  }

  def performHashTags(status: Status): Unit = {
    val hashTags = status.getHashtagEntities
    if (hashTags == null)
      return
    val date = status.getCreatedAt
    val messageId = status.getId
    hashTags foreach {
      hashTag =>
        GraphStorage ! ('save_hash, status.getUser, hashTag.getText, messageId, date)
    }
  }

  val hooks: List[Status => Unit] = List(performRetweet(_), performMentions(_), performUrls(_), performHashTags(_))

  override def onStatus(status: Status) = {
    TweetStorage ! ('index, status)
    hooks foreach {
      h => h(status)
    }
  }

  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {
  }

  override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {
    log.error("Got track limitation notice: %d", numberOfLimitedStatuses)
  }

  override def onScrubGeo(userId: Long, upToStatusId: Long) {
  }

  override def onException(ex: Exception) {
    log.error(ex.getMessage)
    ex.printStackTrace();
  }
}