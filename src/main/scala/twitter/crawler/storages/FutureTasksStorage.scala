package twitter.crawler.storages

import java.util.Deque
import actors.Actor


object FutureTasksStorage extends Actor {

  val MINUTE = 1000 * 60
  val HOUR = 60 * MINUTE
  val DAY = 12 * HOUR

  def act() {}
}
