package twitter.crawler.storages

import java.util.Date

object FutureTasksStorage {
	val URL_KEY="u"
	val RETWEET_KEY="rt"

	val pool: JedisPool = new JedisPool(new JedisPoolConfig(), "localhost");


  def addFutureRetweetTask(statusId: Long, ts: Date, period: Long = 0) = {
  	val jedis = pool.getResource();
  	try {
 			  jedis.zadd(RETWEET_KEY, ts, statusId)
			} finally {
  		pool.returnResource(jedis);
		}


  }

  def getCurrentRtForSearch() = {
  	val now = System.currentTimeInMillis
  	val jedis = pool.getResource()
  	try {
 			  val result = Set(jedis.zrangebyscore(RETWEET_KEY, -inf, ts))
 			  jedis.zrem(RETWEET_KEY, result)
			} finally {
  		pool.returnResource(jedis);
		}
  }

  def getCurrentUrlsForSearch() = {
  	val now = System.currentTimeInMillis
  	val jedis = pool.getResource()
  	try {
 			  val result = Set(jedis.zrangebyscore(URL_KEYURL_KEY, -inf, ts))
 			  jedis.zrem(URL_KEY, result)
			} finally {
  		pool.returnResource(jedis);
		}
  }


  def addFutureUrlTask(url: String, period: Long = 0) = {
  	val jedis = pool.getResource();
  	try {
 			  jedis.zadd(URL_KEY, ts, url)
			} finally {
  		pool.returnResource(jedis);
		}
  }
}
