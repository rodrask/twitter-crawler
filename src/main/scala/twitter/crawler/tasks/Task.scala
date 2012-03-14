package twitter.crawler.tasks

import actors.Actor
import twitter.crawler.common.TwitterService

case class Stop()

case class Pause()

case class Resume()

case class Begin()

trait Task extends Actor {
  var twLimits: Int
  def twitter = TwitterService.defaultTwitter
}