package twitter.crawler.storm

import backtype.storm.tuple.Tuple
import storm.scala.dsl.StormBolt
import twitter4j.Status
import twitter.crawler.storages.StreamStorage._
import twitter.crawler.storages.TweetStorage.saveTweet
import twitter.crawler.storages.GraphStorage._
import twitter.crawler.storages.FutureTasksStorage._
import twitter.crawler.storages.StatisticStorage._
import twitter.crawler.common.extractURL

class TwitterBoltCommon extends StormBolt(outputFields = List()) {

	def performRetweet(status: Status): Unit = {
		if (status.isRetweet)
		{
			val rStatus  = status.getRetweetedStatus
			if (! inIndex(rStatus)){
				saveTweet(rStatus)
			}

			saveRetweet(status.getUser.getScreenName, rStatus.getUser.getScreenName, status.getId, status.getCreatedAt)
			addFutureRetweetTask(rStatus.getId, rStatus.getCreatedAt)
		}


	}
	def performMentions(status: Status): Seq[String] = {
		val mentions = status.getUserMentionEntities
		if (mentions == null)
			return
		mentions foreach {
			mention =>
				saveMention(status.getUser.getScreenName, mention.getScreenName, status.getCreatedAt)
		}
	}

	def performUrls(status: Status): Seq[String] = {
		val urls = status.getURLEntities
		if (urls == null)
			return
		val date = status.getCreatedAt
		urls foreach {
			url =>
				val expandedUrl = extractURL(url)
				saveUrlUsage(expandedUrl, date)
				saveUrlPost(status.getUser.getScreenName, mention.getScreenName, expandedUrl, status.getCreatedAt)
				addFutureUrlTask(expandedUrl)
		}
	}

	def performHashTags(status: Status): Seq[String] = {
		val hashTags = status.getHashtagEntities
		if (hashTags == null)
			return
		val date = status.getCreatedAt
		hashTags foreach {
			hashTag =>
				saveHashTagUsage(hashTag.getText, date)
				saveHashTag(status.getUser.getScreenName, hashTag.getText, date)
		}
	}

	val hooks: List(Status => Unit) = List(performRetweet, performMentions, performUrls, performHashTags)

  def execute(t: Tuple) =
    t matchSeq {
      case Seq(status: Status) =>
        saveTweet(status)
        hooks foreach {
        	h => h(status)
        }
        t ack
    }
  override def cleanup():scala.Unit={
    end
  }
}
