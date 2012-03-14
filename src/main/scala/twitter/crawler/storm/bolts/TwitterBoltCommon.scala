package twitter.crawler.storm.bolts

import backtype.storm.tuple.Tuple
import storm.scala.dsl.StormBolt
import twitter4j.Status
import twitter.crawler.storages.TweetStorage._
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.common.extractURL
import twitter.crawler.storages.FutureTasksStorage

class TwitterBoltCommon extends StormBolt(outputFields = List()) {

  def performRetweet(status: Status): Unit = {
    if (status.isRetweet) {
      val rStatus = status.getRetweetedStatus
      if (!indexed(rStatus)) {
        saveTweet(rStatus)
      }
      saveRetweet(status.getUser, rStatus.getUser, status.getId, rStatus.getId, status.getCreatedAt)
      FutureTasksStorage ! ('put, rStatus.getId)
    }
  }

  def performMentions(status: Status): Unit = {
    val mentions = status.getUserMentionEntities
    if (mentions == null)
      return
    val id = status.getId
    mentions foreach {
      mention =>
        saveMention(status.getUser, mention.getId, mention.getScreenName, id, status.getCreatedAt)
    }
  }

  def performUrls(status: Status): Unit = {
    val urls = status.getURLEntities
    if (urls == null)
      return
    urls foreach {
      url =>
        val expandedUrl = extractURL(url)
        saveUrl(status.getUser, expandedUrl, status.getId, status.getCreatedAt)
        FutureTasksStorage ! ('put, expandedUrl)
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
        saveHashTag(status.getUser, hashTag.getText, messageId, date)
    }
  }

  val hooks: List[Status => Unit] = List(performRetweet(_), performMentions(_), performUrls(_), performHashTags(_))

  def execute(t: Tuple) =
    t matchSeq {
      case Seq(status: Status) =>
        saveTweet(status)
        hooks foreach {
          h => h(status)
        }
        t ack
    }

  override def cleanup(): scala.Unit = {
  }
}
