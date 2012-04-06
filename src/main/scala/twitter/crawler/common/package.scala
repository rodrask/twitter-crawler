package twitter.crawler
import com.codahale.logula.Logging
import org.apache.log4j.Level
import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import twitter4j.URLEntity
import scala.util.matching.Regex
import java.io.FileReader
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

package object common {
	val commonProperties = loadConf("src/main/resources/app.properties")
  val storageProperties = loadConf("src/main/resources/storages.properties")
  val redisPool: JedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379)

	def loadConf(name: String): Map[String, String] = {
		val file = new java.io.FileInputStream(name)
		val properties = new java.util.Properties
		properties.load(file)
		file.close()
		properties
	}

  def loadId(name: String): Seq[Long]={
    val file = scala.io.Source.fromFile(name)
    file.getLines().map(l => l.toLong).toList
  }

  val twiRegex = new Regex("^http://twitter.com/(.*)$")
  def parseURL(address: String): String = {
    val twiRegex(name) = address;
    name
  }

  def loadNames(file: String): List[String]={
    (fromFile(file).getLines map parseURL).toList
  }

  def extractURL(e: URLEntity):String={
    if (e.getExpandedURL != null)
      return e.getExpandedURL.toString
    if (e.getURL != null)
      return e.getURL.toString
    println("Strange entity: "+e)
    null
  }

  Logging.configure { log =>
    log.level = Level.WARN
    log.console.enabled = true
    log.console.threshold = Level.WARN

    log.file.enabled = true
    log.file.filename = storageProperties("logs.storage")
    log.file.maxSize = 100 * 1024 // KB
    log.file.retainedFiles = 5 // keep five old logs around
  }

}