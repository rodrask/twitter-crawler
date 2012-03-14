package twitter.crawler.storages

import actors.Actor
import collection.mutable.PriorityQueue
import scala.collection.mutable.{Set => mSet}


object FutureTasksStorage extends Actor {
  val MINUTE = 1000 * 1
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR

  case class UrlTask(url: String, lastMessage: Option[Long], when: Long, interval: Int)

  implicit val OrderingUrl = Ordering.by((_: UrlTask).when)
  val urlTasks: PriorityQueue[UrlTask] = new PriorityQueue[UrlTask]()(OrderingUrl).reverse
  val urlSet: mSet[String] = mSet[String]()

  case class RTTask(mesageId: Long, when: Long)

  implicit val OrderingRT = Ordering.by((_: RTTask).when)
  val rtTasks: PriorityQueue[RTTask] = new PriorityQueue[RTTask]()(OrderingRT).reverse
  val INTERVALS: List[Long] = List[Long](MINUTE, HOUR, 6 * HOUR, DAY)

  def putRTTasks(messageId: Long): Unit = {
    val now = System.currentTimeMillis
    INTERVALS foreach {
      i => rtTasks += RTTask(messageId, now + i)
    }
  }

  def putUrlTask(url: String, lastMessage: Option[Long] = None, interval: Int = 0): Unit = {
    if (interval > INTERVALS.size || (urlSet contains url))
      return
    urlSet += url
    val now = System.currentTimeMillis
    urlTasks += UrlTask(url, lastMessage, now + INTERVALS(interval), interval + 1)
  }

  def getUrlTask: Option[UrlTask] = {
    if (urlTasks isEmpty)
      None
    else {
      if (urlTasks.head.when < System.currentTimeMillis()) {
        val urlTask = urlTasks.dequeue()
        urlSet -= urlTask.url
        Some(urlTask)
      }
      else
        None
    }
  }

  def getRTTask: Option[RTTask] = {
    if (rtTasks isEmpty)
      None
    else {
      if (rtTasks.head.when < System.currentTimeMillis()) {
        val rtTask = rtTasks.dequeue()
        Some(rtTask)
      }
      else
        None
    }
  }

  def act() {
    loopWhile(true) {
      react {
        case ('put, messageId: Long) =>
          putRTTasks(messageId)
        case ('put, url: String) =>
          putUrlTask(url)
        case ('put, url: String, lastMessage: Long, interval: Int) =>
          putUrlTask(url, Some(lastMessage), interval)
        case 'get_rt =>
          sender ! getRTTask
        case 'get_url =>
          sender ! getUrlTask
        case 'stop =>
          exit
      }
    }

  }
}
