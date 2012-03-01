package twitter.crawler.storages

import java.util.Date
import redis.clients.jedis._
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.Set

object FutureTasksStorage {
  val MINUTE = 1000 * 60
  val HOUR = 60 * MINUTE
  val DAY = 12 * HOUR
  val SEP = "@#@"
  val FORMAT="%s"+SEP+"%s"
  val URL = "u"
  val RT = "rt"

  val pool = new JedisPool(new JedisPoolConfig(), "localhost")

  def addFutureTask[T](key: String)(value: T) = {
    val jedis: Jedis = pool.getResource
    val ts = System.currentTimeMillis()
    try {
      jedis.zadd(RT, ts + MINUTE, FORMAT.format(value.toString, "MINUTE"))
      jedis.zadd(RT, ts + HOUR, FORMAT.format(value.toString, "MINUTE"))
      jedis.zadd(RT, ts + DAY, FORMAT.format(value.toString, "MINUTE"))
    } finally {
      pool.returnResource(jedis);
    }
  }

  def addRTFutureTask = addFutureTask[Long](RT)
  def addURLFutureTask = addFutureTask[String](URL)

  
  def getCurrentTaskForSearch[T](key: String): Set[T] = {
    val now = System.currentTimeMillis()
    val jedis = pool.getResource()
    var result: Set[T] = _
    try {
      result = jedis.zrangeByScore(RT, 0, now) map {
        s => (s.split(SEP)(0)).asInstanceOf[T]
      }
      jedis.zremrangeByScore(RT, 0, now)
    } finally {
      pool.returnResource(jedis);
    }
    return result

  }
  def getCurrentUrlsForSearch = getCurrentTaskForSearch[String](URL)
  def getCurrentRtForSearch = getCurrentTaskForSearch[Long](RT)
}
