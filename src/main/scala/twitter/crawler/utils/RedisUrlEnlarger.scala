package twitter.crawler.utils

import com.codahale.logula.Logging
import dispatch.thread.Safety
import dispatch.{Http, Request, url}
import dispatch.XhtmlParsing._
import java.io.FileWriter
import scala.collection.mutable.{Map => mMap}
import util.matching.Regex
import io.Source
import java.util.Date
import twitter.crawler.common.redisPool

object RedisUrlEnlarger extends Logging {
  val DEFAULT_FILE = "src/main/resources/services.properties"
  val HTTP_PREFIX = "http[s]{0,1}://"
  val h = new Http with Safety {

    import org.apache.http.params.CoreConnectionPNames

    client.getParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000)
    client.getParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
  }

  val serviceRequest = url("http://api.longurl.org/v2/services") <:< Map("User-Agent" -> "Twitter-crawler/0.1")
  val services: List[Regex] = loadShortServices()

  def saveShortServices(filename: String = DEFAULT_FILE) = {
    val services = h(serviceRequest </> {
      nodes => (nodes \\ "service") map (_.text)
    })

    val file = new FileWriter(filename, false)
    services foreach {
      s => file.write("%s\n".format(s))
    }
    file.close()
  }

  def shorten(url: String) = {
    services exists {
      r => r.findFirstIn(url).isDefined
    }
  }

  def loadShortServices(filename: String = DEFAULT_FILE): List[Regex] = {
    Source.fromFile(filename).getLines.toList.map(l => new Regex(HTTP_PREFIX + l))
  }

  val enlargeRequest: Request = url("http://api.longurl.org/v2/expand") <:< Map("User-Agent" -> "Twitter-crawler/0.1")


  def fresh(url: String, ts: Date): Boolean = {
    new Date before ts
  }

  val CACHE_PREFIX = "CACHE:"
  val HOUR = 3600

  private def cacheUrl(shortUrl: String, longUrl: String) = {
    val key = CACHE_PREFIX + shortUrl
    val jedis = redisPool.getResource
    try {
      jedis.set(key, longUrl)
      jedis.expire(key, 2 * HOUR)
    }
    finally {
      redisPool.returnResource(jedis)
    }
  }

  private def fromCache(shortUrl: String): Option[String] = {
    val key = CACHE_PREFIX + shortUrl
    val jedis = redisPool.getResource
    try {
      val url = jedis.get(key)
      if (url == null)
        None
      else
        Some(url)
    } finally {
      redisPool.returnResource(jedis)
    }
  }

  def enlarge(url: String): String = {
    if (shorten(url)) {
      val cached = fromCache(url)
      if (cached.isDefined) {
        log.info("Url in cache: %s -> %s", url, cached.get)
        cached.get
      }
      else {
        log.info("New url: %s", url)
        var isOk: Boolean = true
        val eUrl: String = h x (enlargeRequest <<? Map("url" -> url) <> {
          n => (n \\ "long-url").head.text
        }) {
          case (200, _, _, out) => out()
          case (s, _, _, out) =>
            log.warn("Bad status: %d", s)
            isOk = false
            url
        }
        log.info("Url %s extracted to %s", url, eUrl)
        cacheUrl(url, eUrl)
        eUrl
      }
    } else {
      url
    }
  }
}
