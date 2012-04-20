package twitter.crawler.storages

import org.neo4j.scala.{EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import org.neo4j.cypher.{ExecutionResult, CypherParser, ExecutionEngine}
import org.neo4j.graphdb.Node

case class UrlRawFactors(timestamps: List[Long], users: List[Node])
trait NeoQueriesTrait extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {
  val cypherParser = new CypherParser
  val engine = new ExecutionEngine(ds.gds);


  private def makeQuery(query: String, params: Map[String, Any]): ExecutionResult = {
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

  def getUrlTimestamps(url: String, from: Long, to: Long): List[Long] = {
    val query = """
    start url=node:entities(name={url})
    match ()-[r:POSTED]->url
     where r.ts>={from} and r.ts<={to}
    return url.name as url, collect(r.ts) as timestamps"""
    extractList apply makeQuery(query, Map("url" -> url, "from" -> from, "to" -> to))
  }

  val extractNameAndList: ExecutionResult => (String, List[Long]) = {
    result: ExecutionResult =>
      if (!result.hasNext) {
        ("None", List.empty[Long])
      } else {
        val map = result.next()
        val ts = map.getOrElse("timestamps", None) match {
          case l: List[Long] => l
          case _ => List.empty[Long]
        }
        val name = map.getOrElse("name", "None") match {
          case n: String => n
          case _ => "None"
        }
        (name, ts)
      }
  }

  def getUserUrlsTs(twId: Long, from: Long, to: Long): (String, List[Long]) = {
    val query = """
		start user=node:users(twId={id})
		match user-[r:POSTED]->()
		 where r.ts>={from} and r.ts<={to}
		return user.name as name, collect(r.ts) as timestamps
		"""
    extractNameAndList apply makeQuery(query, Map("id" -> twId, "from" -> from, "to" -> to))
  }

  def getUsersUrlsTs(ids: List[Long], from: Long, to: Long): Seq[(String, List[Long])] = {
    val query = """
		start user=node({ids})
		match user-[r:POSTED]->()
		 where r.ts>={from} and r.ts<={to}
		return user.name as name, collect(r.ts) as timestamps
		"""
    extractNameAndList apply makeQuery(query, Map("id" -> twId, "from" -> from, "to" -> to))
  }

//  def getUrlSubgraph(url: String, from: Long, to: Long): List[Node] = {
//    val query = """
//		start url=node:entities(name={url}) match user-[r:POSTED]->url return url.name as url, collect(distinct user.name) as result
//		"""
//    val result = engine.execute(query, Map("url" -> url, "from" -> from, "to" -> to))
//    extractFunction(result)
//  }

  val extractFactors: ExecutionResult => UrlRawFactors  ={
    result: ExecutionResult =>
      if (!result.hasNext) {
        null
      } else {
        val map = result.next()
        val ts = map.getOrElse("timestamps", None) match {
          case l: List[Long] => l.sorted
          case _ => List.empty[Long]
        }
        val nodes = map.getOrElse("nodes", None) match {
          case n: List[Node] => n
          case _ => List.empty[Node]
        }
        UrlRawFactors(ts, nodes)
      }
  }
  def getUrlFactors(url: String, from: Long, to: Long): UrlRawFactors = {
    val query = """
    start url=node:entities(name={url}) match user-[r:POSTED]->url
    where r.ts >= {from} and r.ts <= {to}
    return collect(r.ts) as timestamps,
           collect(distinct user) as nodes
    """
    extractFactors apply makeQuery(query, Map("url" -> url, "from" -> from, "to" -> to))
  }

  def
}