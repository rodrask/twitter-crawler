package twitter.crawler.storages

import twitter.crawler.common.commonProperties
import scala.collection.JavaConversions._
import sys.ShutdownHookThread
import org.neo4j.scala.{DatabaseService, EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import twitter4j.User
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.graphdb.index.UniqueFactory
import java.util.{Map => JavaMap, Date}
import org.neo4j.index.lucene.ValueContext

object GraphStorage extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {
  val USER_ID = "twId"

  override def neo4jStoreDir = commonProperties("graphdb.path")

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

  private def getOrCreateUniqueUser(username: String) = {
    val factory: UniqueFactory[Node] = new UniqueFactory.UniqueNodeFactory(userIndex) {
      def initialize(created: Node, properties: JavaMap[String, AnyRef]) {
        created("name") = properties get "name"
        created("nodeType") = "USER"
      }
    }
    factory.getOrCreate("name", username)
  }

  private def getOrCreateUniqueEntity(name: String) = {
    val factory: UniqueFactory[Node] = new UniqueFactory.UniqueNodeFactory(entitiesIndex) {
      def initialize(created: Node, properties: JavaMap[String, AnyRef]) {
        created("name") = properties get "name"
        created("nodeType") = "ENTITY"
      }
    }
    factory.getOrCreate("name", name)
  }

  private def getOrCreateUniqueUser(userId: Long) = {
    val factory: UniqueFactory[Node] = new UniqueFactory.UniqueNodeFactory(userIndex) {
      def initialize(created: Node, properties: JavaMap[String, AnyRef]) {
        created(USER_ID) = properties get USER_ID
        created("nodeType") = "USER"
      }
    }
    factory.getOrCreate(USER_ID, new ValueContext(userId) indexNumeric)
  }

  def indexUserInfo(user: User): Node = {
    val userNode = getOrCreateUniqueUser(user.getScreenName)
    if (userNode("i").isEmpty) {
      userNode("creationDate") = user.getCreatedAt.getTime
      userNode("i") = 1

      userIndex +=(userNode, USER_ID, new ValueContext(user.getId) indexNumeric )
      userIndex +=(userNode, "creationDate", new ValueContext(user.getCreatedAt.getTime) indexNumeric)
    }
    userNode
  }

  def saveEvent(ttype: String, rel: Relationship, id: Long, when: Date) = {
    rel("ts") = when.getTime
    rel("messageId") = id

    eventsIndex +=(rel, "type", ttype)
    eventsIndex +=(rel, "messageId", new ValueContext(id) indexNumeric())
    eventsIndex +=(rel, "ts", new ValueContext(when getTime) indexNumeric())
  }

  def saveRetweet(fromU: User, toU: User, thisMessageId: Long, baseMessageId: Long, when: Date) = {
    val fromN: Node = indexUserInfo(fromU)
    val toN: Node = indexUserInfo(toU)
    withTx {
      implicit ds: DatabaseService =>
        val rel: Relationship = fromN --> "RT" --> toN <()
        saveEvent("RT", rel, thisMessageId, when)

        rel("baseMessageId") = baseMessageId
        eventsIndex +=(rel, "baseMessage", new ValueContext(baseMessageId) indexNumeric())
        println("Save Retweet")
    }
  }

  def saveMention(fromU: User, toScreenName: String, messageId: Long, when: Date) = {
    val fromN: Node = indexUserInfo(fromU)
    val toN: Node = getOrCreateUniqueUser(toScreenName)
    withTx {
      implicit ds: DatabaseService =>
        val rel: Relationship = fromN --> "MENTION" --> toN <()
        saveEvent("MENTION", rel, messageId, when)
        println("Save mention")
    }

  }

  def saveUrl(user: User, url: String, messageId: Long, when: Date) = {
    val userNode = indexUserInfo(user)
    val urlNode = getOrCreateUniqueEntity(url)
    withTx {
      implicit ds: DatabaseService =>
        val rel: Relationship = userNode --> "POSTED" --> urlNode <()
        saveEvent("POSTED", rel, messageId, when)
        println("Save url")
    }

  }

  def saveHashTag(user: User, hashTag: String, messageId: Long, when: Date) = {
    val userNode = indexUserInfo(user)
    val tagNode = getOrCreateUniqueEntity(hashTag)
    withTx {
      implicit ds: DatabaseService =>

        val rel: Relationship = userNode --> "TAGGED" --> tagNode <()
        saveEvent("TAGGED", rel, messageId, when)
        println("Save Hash tag")
    }
  }

  def saveFriendship(from: Long, to: Seq[Long]) = {
    val fromN = getOrCreateUniqueUser(from)
    withTx {
      implicit ds: DatabaseService =>
        to foreach {
          friend:Long =>
            val toN = getOrCreateUniqueUser(friend)
            fromN --> "READS" --> toN
        }
        println("Save friendship")
    }
  }
}
