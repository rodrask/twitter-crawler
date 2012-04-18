package twitter.crawler.storages

import org.neo4j.scala.{EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import org.neo4j.cypher.{ExecutionResult, CypherParser, ExecutionEngine}

trait NeoQueriesTrait extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {

  val cypherParser = new CypherParser
  val engine = new ExecutionEngine(ds.gds);


  private def query(query: String, params: Map[String, AnyRef]) = {
    engine.execute(query)
  }

  def getUrlTimestampts(url: String, from: Long, to: Long) = {
    val extractFunction: ExecutionResult => AnyRef = {
      result: ExecutionResult =>
        result.next().get("result")
    }
    val query = """
		start url=node:entities(name={url}) match ()-[r:POSTED]->url return url.name as url, collect(r.ts) as result
		"""
    val result = engine.execute(query, Map("url" -> url, "from" -> from, "to" -> to))
    extractFunction(result)
  }



  def getUserUrlsPosts(twId: Long, from: Long, to: Long )={
    val extractFunction: ExecutionResult => AnyRef = {
      result: ExecutionResult =>
        result.next().get("result")
    }

    val query = """
		start user=node:users(twId={id}) match user-[r:POSTED]->() return user.name as name, collect(r.ts) as result
		"""
    val result = engine.execute(query, Map("id" -> twId, "from" -> from, "to" -> to))
    extractFunction(result)
  }

  def urlSubGraph(url: String, from: Long, to: Long)={
    val extractFunction: ExecutionResult => AnyRef = {
      result: ExecutionResult =>
        result.next().get("result")
    }
    val query = """
		start url=node:entities(name={url}) match user-[r:POSTED]->url return url.name as url, collect(distinct user.name) as result
		"""
    val result = engine.execute(query, Map("url" -> url, "from" -> from, "to" -> to))
    extractFunction(result)
  }
}