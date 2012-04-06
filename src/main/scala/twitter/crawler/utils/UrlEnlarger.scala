package twitter.crawler.utils

import com.codahale.logula.Logging
import dispatch.thread.Safety
import dispatch.{Http, Request, url}
import dispatch.XhtmlParsing._
import java.io.FileWriter
import scala.collection.mutable.{Map => mMap}
import util.matching.Regex
import io.Source
import actors.Actor
import java.util.{Calendar, Date}

object UrlEnlarger extends Logging {
  val DEFAULT_FILE = "src/main/resources/services.properties"
  val HTTP_PREFIX = "http[s]{0,1}://"
  val h = new Http with Safety {

    import org.apache.http.params.CoreConnectionPNames

    client.getParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000)
    client.getParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
  }

  val serviceRequest = url("http://api.longurl.org/v2/services") <:< Map("User-Agent" -> "Twitter-crawler/0.1")

  var urlCache = mMap[String, String]()
  var urlCacheExpiration = mMap[String, Date]()

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
    Source.fromFile(filename).getLines.toList.map(l => new Regex(HTTP_PREFIX+l))
  }

  val enlargeRequest: Request = url("http://api.longurl.org/v2/expand") <:< Map("User-Agent" -> "Twitter-crawler/0.1")


  def fresh(url: String, ts: Date): Boolean = {
    new Date before ts
  }

  def clean = {
    log.info("Strart clean url cache current size: %d recs", urlCacheExpiration.size)
    println("Clean short urls cache")
    urlCacheExpiration.retain((url, ts) => fresh(url, ts))
    urlCache.retain((url, _) => urlCacheExpiration contains url)
    log.info("End clean url cache current size: %d recs", urlCacheExpiration.size)
  }


  def enlarge(url: String): String = {
    if (shorten(url)) {
      if (urlCache.contains(url) && fresh(url, urlCacheExpiration(url))) {
        log.info("Url in cache: %s", url)
        urlCache(url)
      }
      else {
        log.info("New url: %s", url)
        var isOk: Boolean = true

        val eUrl: String = h x (enlargeRequest <<? Map("url" -> url) <> {
          n => (n \\ "long-url").head.text
        }) {
          case (200, _, _, out) => out()
          case (s, _, _, out) =>
            log.info("Bad status: %d", s)
            isOk = false
            url
        }

        if (isOk) {
          log.info("Url %s extracted to %s", url, eUrl)
          urlCache(url) = eUrl

          val calendar = Calendar.getInstance()
          calendar.add(Calendar.HOUR, 2)
          urlCacheExpiration(url) = calendar.getTime
          if (urlCache.size > 3000)
            clean
        }
        eUrl
      }
    }
    else
      url
  }

}
