package twitter.crawler.storages
import twitter.crawler.common.redisPool
object RedisTsStorage {
  val URL_PREFIX = "URL#"
  val USER_PREFIX = "USER#"

  def addUrlTimestamp(url: String, timestamps: Seq[Long])={
    val jedis = redisPool.getResource
    timestamps foreach(ts => jedis.sadd(URL_PREFIX+url, ts.toString))
  }
}
