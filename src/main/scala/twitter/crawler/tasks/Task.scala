package twitter.crawler.tasks

import actors.Actor
import ru.hse.twitter.common.TwitterService
import twitter4j.{TwitterStream, Twitter}
import twitter.crawler.common.TwitterService

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 30.01.12
 * Time: 2:40
 * To change this template use File | Settings | File Templates.
 */

case class Stop()

case class Pause()

case class Resume()

case class Begin()

trait Task extends Actor {
  var twLimits: Int
  def twitter = TwitterService.twitter
}