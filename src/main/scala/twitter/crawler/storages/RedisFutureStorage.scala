package twitter.crawler.storages

import actors.Actor
import redis.clients.jedis.{JedisPoolConfig, JedisPool}
import twitter.crawler.storages.FutureTasksStorage.{UrlTask, RTTask}
import scala.collection.JavaConversions._

object RedisFutureStorage extends Actor {
  val MINUTE = 1000 * 1
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val pool: JedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379)
  val INTERVALS: List[Long] = List[Long](MINUTE, HOUR, 6 * HOUR, DAY)

  def putRTTask(messageId: Long) = {
    val jedis = pool.getResource();
    val key = messageId.toString
    try {
      if (!jedis.hexists(key, "interval")) {
        val now = System.currentTimeMillis()
        jedis.zadd("RT", now + MINUTE, key)
        jedis.hmset(key, Map("interval" -> "1", "ts" -> now.toString))
      }
    } finally {
      pool.returnResource(jedis);
    }
  }

  def putUrlTask(url: String, lastMessage: Option[Long]) = {
    val jedis = pool.getResource();
    try {
      if (!jedis.hexists(url, "interval")) {
        val now = System.currentTimeMillis()
        jedis.zadd("URL", now + MINUTE, url)
        jedis.hmset(url, Map("interval" -> "1", "ts" -> now.toString))
        if (lastMessage.isDefined)
          jedis.hset(url, "lastMessage", lastMessage.toString)
      }
    } finally {
      pool.returnResource(jedis);
    }
  }

  def getRTTask(): Option[Long] = {
    val jedis = pool.getResource()
    val now = System.currentTimeMillis()
    try {
      val result = jedis.zrangeWithScores("RT", 0, 1)
      if (result.size == 0)
        return None
      val tuple = result.iterator().next()
      if (tuple.getScore > now)
        return None
      val messageId = tuple.getElement()
      jedis.hget(messageId, "interval") match {
        case null => println("Something strange")
        case "3" =>
          jedis.hdel(messageId, "interval")
          jedis.hdel(messageId, "ts")
          jedis.zrem("RT", messageId)
        case interval: String =>
          jedis.hincrBy(messageId, "interval", 1)
          val newTs = INTERVALS(interval.toInt) + jedis.hget(messageId, "ts").toLong
          jedis.zincrby("RT", newTs - tuple.getScore, messageId)
      }
      Some(messageId.toLong)
    }

    finally {
      pool.returnResource(jedis)
    }
  }

  def getUrlTask(): Option[(String, Option[Long])] = {
    val jedis = pool.getResource()
    val now = System.currentTimeMillis()
    try {
      val result = jedis.zrangeWithScores("URL", 0, 1)
      if (result.size == 0)
        return None
      val tuple = result.iterator().next()
      if (tuple.getScore > now)
        return None
      val url = tuple.getElement()
      jedis.hget(url, "interval") match {
        case null => println("Something strange")
        case "3" =>
          jedis.hdel(url, "interval")
          jedis.hdel(url, "ts")
          jedis.hdel(url, "lastMessage")
          jedis.zrem("URL", url)
        case interval: String =>
          jedis.hincrBy(url, "interval", 1)
          val newTs = INTERVALS(interval.toInt) + jedis.hget(url, "ts").toLong
          jedis.zincrby("URL", newTs - tuple.getScore, url)
      }
      val lastMessage = jedis.hget(url, "lastMessage")
      Some((url, if (lastMessage == null) None else Some(lastMessage.toLong)))
    }
    finally {
      pool.returnResource(jedis)
    }
  }

  def act() {
    loopWhile(true) {
      react {
        case ('put, messageId: Long) =>
          putRTTask(messageId)
        case ('put, url: String) =>
          putUrlTask(url, None)
        case ('put, url: String, lastMessage: Long, interval: Int) =>
          putUrlTask(url, Some(lastMessage))
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
