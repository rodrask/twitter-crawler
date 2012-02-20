package twitter.crawler

import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import util.matching.Regex
import twitter4j.URLEntity

package object common {
	val properties = loadConf("app.properties")

	def loadConf(name: String): Map[String, String] = {
		val file = new java.io.FileInputStream(name)
		val properties = new java.util.Properties
		properties.load(file)
		file.close()
		properties
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
}