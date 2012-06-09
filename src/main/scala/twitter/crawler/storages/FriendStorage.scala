package twitter.crawler.storages

import twitter.crawler.common.storageProperties
import com.codahale.logula.Logging
import scala.collection.JavaConversions._
import sys.ShutdownHookThread
import org.neo4j.scala.{DatabaseService, EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import org.neo4j.graphdb.index.UniqueFactory
import java.util.{Map => JavaMap, Date}
import org.neo4j.index.lucene.ValueContext
import org.neo4j.graphdb._
import actors.Actor
import twitter4j.User
import collection.immutable.SortedSet
import java.io.Writer
import org.neo4j.cypher.ExecutionResult

object FriendStorage extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider with NeoQueriesTrait with Logging {
  val USER_ID = "twId"
  val MESSAGE_ID = "messageId"
  val UNKNOWN = "unknown"

  override def neo4jStoreDir = storageProperties("friend.storage")

  val indexProperties: IndexCustomConfig = Some(Map("provider" -> "lucene", "type" -> "exact"))

  override def NodeIndexConfig = ("users", indexProperties) ::("entities", indexProperties) :: Nil

  override def RelationIndexConfig = ("events", indexProperties) :: Nil

  def userIndex = getNodeIndex("users").get

  def entitiesIndex = getNodeIndex("entities").get

  def eventsIndex = getRelationIndex("events").get

  ShutdownHookThread {
    println("shutdown hook")
    shutdown(ds)
  }

  def stopStorage = {
    exit()
    shutdown(ds)
  }

  def isRead(user1:String, user2: String): Boolean={
    val query =
      """
        start fromUser=node:users(name={from}), toUser=node:users(name={to})
        match fromUser-[r:READS]->toUser
        return count(r) as result
      """
    val extractFunc: ExecutionResult => Boolean = {
      result =>
        if (result.hasNext)
          !result.next().isEmpty
        else
          false
    }
    extractFunc apply makeQuery(query, Map("from" -> user1, "to" -> user2))
  }

  def indexUsers()={
    withTx{
      implicit db =>
        getAllNodes foreach{
          node: Node =>
            val name = node("name")
            if (name.isDefined){
              userIndex += (node, "name", name.get)
              println("indexed %s".format(name.get))
            }
            else{
              println("No name for %s".format(node.toString))
            }

        }
    }

  }
  def followers(user: String): Set[String]={
    val query =
      """
        start user=node:users(name={user})
        match fromUser-[r:READS]->user
        where has(fromUser.name)
        return collect(fromUser.name?) as user
      """
    val extractFunc: ExecutionResult => Set[String] = {
      result =>
        result.map[String](row => row("user").toString).toSet
    }
    extractFunc apply makeQuery(query, Map("user" -> user))
  }
}
