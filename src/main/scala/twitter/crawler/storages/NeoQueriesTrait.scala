package twitter.crawler.storages

import org.neo4j.scala.{EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import scala.Long
import scala.Predef._
import collection.immutable.SortedSet
import scala.collection.JavaConverters._
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.index.lucene.ValueContext
import org.neo4j.graphdb._
import org.neo4j.cypher.{ExecutionResult, CypherParser, ExecutionEngine}

case class UrlData(timestamps: SortedSet[Long], users: List[String])

trait NeoQueriesTrait extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {
  val cypherParser = new CypherParser
  val engine = new ExecutionEngine(ds.gds);

  protected def makeQuery(query: String, params: Map[String, Any]): ExecutionResult = {
    engine.execute(query, params)
  }

  val extractList: ExecutionResult => List[Long] = {
    result: ExecutionResult =>
      if (!result.hasNext) {
        List.empty[Long]
      } else {
        result.next().getOrElse("timestamps", None) match {
          case l: List[Long] => l
          case _ => List.empty[Long]
        }
      }
  }


  val extractNameAndList: ExecutionResult => (String, List[Long]) = {
    result: ExecutionResult =>
      if (!result.hasNext) {
        ("None", List.empty[Long])
      } else {
        val map = result.next()
        val ts = map.getOrElse("timestamps", None) match {
          case l: List[Long] => l map (ts => ts / 1000)
          case _ => List.empty[Long]
        }
        val name = map.getOrElse("name", "None") match {
          case n: String => n
          case _ => "None"
        }
        (name, ts)
      }
  }

  val extractUserAndUrls: ExecutionResult => (String, List[String]) = {
    result: ExecutionResult =>
      if (!result.hasNext) {
        ("None", List.empty[String])
      } else {
        val map = result.next()
        val ts = map.getOrElse("urls", None) match {
          case l: List[String] => l
          case _ => List.empty[String]
        }
        val name = map.getOrElse("name", "None") match {
          case n: String => n
          case _ => "None"
        }
        (name, ts)
      }
  }

  val extractNamesAndList: ExecutionResult => Map[String, SortedSet[Long]] = {
    result: ExecutionResult =>
      var toReturn: Map[String, SortedSet[Long]] = Map[String, SortedSet[Long]]()
      for (row: Map[String, Any] <- result) {
        val name = row.getOrElse("name", "None") match {
          case n: String => n
          case _ => "None"
        }
        val ts: SortedSet[Long] = row.getOrElse("timestamps", None) match {
          case l: List[Long] => SortedSet(l.map(ts => ts / 1000): _*)
          case _ => SortedSet.empty[Long]
        }
        toReturn = toReturn + ((name, ts))
      }
      toReturn
  }

  def extractSortedSet[T <% Ordered[T]](field: String, mapF: T => T)(row: Map[String, Any]): SortedSet[T] = {
    row.getOrElse(field, None) match {
      case l: List[T] => SortedSet[T](l map mapF: _*)
      case _ => SortedSet.empty[T]
    }
  }

  def extractList[T](field: String)(row: Map[String, Any]): List[T] = {
    row.getOrElse(field, None) match {
      case l: List[T] => l
      case _ => List.empty[T]
    }
  }

  def extractProperty(field: String)(row: Map[String, Any]): String = {
    row.getOrElse(field, None) match {
      case l: String => l
      case _ => "None"
    }
  }

  val toSec: Long => Long = x => x / 1000
  val extractTimestamps: Map[String, Any] => SortedSet[Long] = extractSortedSet[Long]("timestamps", toSec)(_)
  val extractNodes: Map[String, Any] => List[String] = extractList[String]("nodes")(_)
  val extractName: Map[String, Any] => String = extractProperty("name")(_)

  val extractUrlsFactors: ExecutionResult => Iterator[(String, UrlData)] = {
    result: ExecutionResult =>
      for (row: Map[String, Any] <- result)
      yield (extractName(row), UrlData(extractTimestamps(row), extractNodes(row)))
  }

  val extractSingleUrlFactors: ExecutionResult => UrlData = {
    result: ExecutionResult =>
      if (!result.hasNext) {
        null
      } else {
        val row = result.next()
        UrlData(extractTimestamps(row), extractNodes(row))
      }
  }


  def getUrlTimestamps(url: String, from: Long, to: Long): List[Long] = {
    val query = """
    start url=node:entities(name={url})
    match ()-[r:POSTED]->url
     where r.ts>={from} and r.ts<={to}
    return url.name as url, collect(r.ts) as timestamps"""
    extractList apply makeQuery(query, Map("url" -> url, "from" -> from, "to" -> to))
  }

  def getUserUrlsTs(name: String, from: Long = 0, to: Long = Long.MaxValue): (String, List[Long]) = {
    val query = """
		start user=node:users(name={name})
		match user-[r:POSTED]->()
		 where r.ts>={from} and r.ts<={to}
		return user.name as name, collect(r.ts) as timestamps
                		"""
    extractNameAndList apply makeQuery(query, Map("name" -> name, "from" -> from, "to" -> to))
  }

  def getUserUrls(name: String, from: Long = 0, to: Long = Long.MaxValue): (String, List[String]) = {
    val query = """
		start user=node:users(name={name})
		match user-[r:POSTED]->url
		 where r.ts>={from} and r.ts<={to}
		return user.name as name, collect(url.name) as urls
                		"""
    extractUserAndUrls apply makeQuery(query, Map("name" -> name, "from" -> from, "to" -> to))
  }

  def getUrlSubgraph(url: String, from: Long, to: Long): Map[String, SortedSet[Long]] = {
    val query = """
		start url=node:entities(name={url}) match url<-[r:POSTED]-user-[rr:POSTED]->()
    where r.ts>={from} and r.ts<={to}
		return user.name as name, collect(rr.ts) as timestamps
                		"""
    val result = engine.execute(query, Map("url" -> url, "from" -> from, "to" -> to))
    extractNamesAndList(result)
  }

  def dumpUrlFactors(from: Long, to: Long, skip: Int, limit: Int): Iterator[(String, UrlData)] = {
    val query = """
    start url=node:entities("name:*") match user-[r:POSTED]->url
    where r.ts! >= {from} and r.ts <= {to}
    return url.name? as name, collect(r.ts) as timestamps,
           collect(user.name) as users
                """
    extractUrlsFactors apply makeQuery(query, Map("skip" -> skip, "limit" -> limit, "from" -> from, "to" -> to))
  }


  def getUrlFactors(url: String, from: Long = 0, to: Long = Long.MaxValue): UrlData = {
    val query = """
    start url=node:entities(name={url}) match user-[r:POSTED]->url
    where r.ts >= {from} and r.ts <= {to}
    return collect(r.ts) as timestamps,
           collect(user) as nodes
                """
    extractSingleUrlFactors apply makeQuery(query, Map("url" -> url, "from" -> from, "to" -> to))
  }

  def numberOfRT(from: String, to: String): Long ={
    val query =
      """
        start fromUser=node:users(name={from}), toUser=node:users(name={to})
        match fromUser-[r:RT]->toUser
        return count(r) as result
      """
    val extractFunc: ExecutionResult => Long = {
      result =>
        if (result.hasNext)
          result.next().getOrElse("result", 0).asInstanceOf[Long]
        else
          0
    }
    extractFunc apply makeQuery(query, Map("from" -> from, "to" -> to))
  }

  def urlsStream(): Iterable[String] = {
    val relType = DynamicRelationshipType.withName("POSTED")
    GlobalGraphOperations.at(ds.gds).getAllNodes.asScala.
      filter(n => n.getProperty("nodeType", "") == "ENTITY" && n.getRelationships(Direction.INCOMING, relType).asScala.size > 10).
      map(n => n.getProperty("name").toString )
  }
}