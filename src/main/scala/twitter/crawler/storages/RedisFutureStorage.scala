package twitter.crawler.storages

import actors.Actor
import twitter.crawler.storages.FutureTasksStorage.{UrlTask, RTTask}
import twitter.crawler.common.redisPool
import scala.collection.JavaConversions._
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisPool}

object RedisFutureStorage{
  val MINUTE = 60 * 1000
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val INTERVALS: List[Long] = List[Long](MINUTE, HOUR - MINUTE, 5 * HOUR, 19*HOUR)

  val RT_TASK = "RT"
  val URL_TASK = "URL"
  val INTERVAL_FIELD = "int"
  val LAST_MESSAGE_FIELD = "lm"

  private def insertTask(jedis: Jedis ,key: String, value: String, intIndex: Int)={
    jedis.zadd(key, System.currentTimeMillis() + INTERVALS(intIndex), value)
    jedis.hincrBy(value, INTERVAL_FIELD, 1)
  }
  private def delTask(jedis: Jedis ,key: String, value: String)={
    jedis.zrem(key, value)
    jedis.hdel(value, INTERVAL_FIELD)
  }
  private def inStorage(jedis: Jedis, value: String): Boolean = jedis.hexists(value, INTERVAL_FIELD)

  def putRTTask(messageId: Long) = {
    val jedis = redisPool.getResource();
    val storedValue = messageId.toString
    try {
      if (!inStorage(jedis, storedValue))
        insertTask(jedis, RT_TASK, storedValue, 0)
    } finally {
      redisPool.returnResource(jedis);
    }
  }

  def getRTTask(): Option[Long]={
    val jedis = redisPool.getResource()
    try {
      val result = jedis.zrangeWithScores(RT_TASK, 0, 1)
      if (result.isEmpty)
        return None
      val tuple = result.iterator().next()
      if (System.currentTimeMillis < tuple.getScore)
        return None
      val messageId = tuple.getElement
      val intervalIndex = jedis.hget(messageId, INTERVAL_FIELD).toInt
      if (intervalIndex >= 3){
        delTask(jedis, RT_TASK, messageId)
      }
      else{
        insertTask(jedis, RT_TASK, messageId, intervalIndex)
      }
      return Some(messageId.toLong)
    } finally{
      redisPool.returnResource(jedis);
    }
  }

  def putUrlTask(url: String, lastMessage: Option[Long]) = {
    val jedis = redisPool.getResource();
    try {
      if (!inStorage(jedis, url)){
        insertTask(jedis, URL_TASK, url, 0)
        if (lastMessage.isDefined)
          jedis.hset(url, LAST_MESSAGE_FIELD, lastMessage.toString)
      }
    } finally {
      redisPool.returnResource(jedis);
    }
  }

  def getUrlTask(): Option[(String, Option[Long])] = {
    val jedis = redisPool.getResource()
    try {
      val result = jedis.zrangeWithScores(URL_TASK, 0, 1)
      if (result.isEmpty)
        return None
      val tuple = result.iterator().next()
      if (System.currentTimeMillis < tuple.getScore)
        return None
      val url = tuple.getElement

      val lMField = jedis.hget(url, LAST_MESSAGE_FIELD)
      val lastMessage = if (lMField == null) None else Some(lMField.toLong)

      val intervalIndex = jedis.hget(url, INTERVAL_FIELD).toInt
      if (intervalIndex >= 3){
        delTask(jedis, URL_TASK, url)
        jedis.hdel(url, LAST_MESSAGE_FIELD)
      }
      else{
        insertTask(jedis, URL_TASK, url, intervalIndex)
      }
      return Some(url, lastMessage)
    } finally{
      redisPool.returnResource(jedis);
    }
  }

  def updateLastMessageUrl(url: String, lastMessage: Long)={
    val jedis = redisPool.getResource()
    try {
      if (inStorage(jedis, url))
      {
        jedis.hset(url, LAST_MESSAGE_FIELD, lastMessage.toString)
      }
    } finally{
      redisPool.returnResource(jedis);
    }
  }
}
