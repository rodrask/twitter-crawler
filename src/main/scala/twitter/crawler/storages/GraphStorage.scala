package twitter.crawler.storages

import twitter.crawler.common.commonProperties
import scala.collection.JavaConversions._
import sys.ShutdownHookThread
import org.neo4j.scala.{DatabaseService, EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import twitter4j.User
import org.neo4j.graphdb.index.UniqueFactory
import java.util.{Map => JavaMap, Date}
import org.neo4j.index.lucene.ValueContext
import org.neo4j.graphdb.{DynamicRelationshipType, Direction, Relationship, Node}

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
        created(USER_ID) = properties(USER_ID).asInstanceOf[ValueContext].getValue
        created("nodeType") = "USER"
      }
    }
    factory.getOrCreate(USER_ID, new ValueContext(userId))
  }

  def indexUserInfo(user: User): Node = {
    val userNode = getOrCreateUniqueUser(user.getId)
    if (userNode("i").isEmpty) {
      userNode("name") = user.getScreenName
      userNode("creationDate") = user.getCreatedAt.getTime

      val location = if (user.getLocation == null) "" else user.getLocation
      val lang = if (user.getLang == null) "" else user.getLang
      val description = if (user.getDescription == null) "" else user.getDescription

      userNode("location") = location
      userNode("lang") = lang
      userNode("description") = description
      userNode("followersCount") = user.getFollowersCount
      userNode("friendsCount") = user.getFriendsCount
      userNode("statusesCount") = user.getStatusesCount
      userNode("isProtected") = user.isProtected

      userNode("i") = 1

      userIndex +=(userNode, "name", user.getScreenName)
      userIndex +=(userNode, "creationDate", new ValueContext(user.getCreatedAt.getTime) indexNumeric)
    }
    userNode
  }

  private def saveEvent(ttype: String, rel: Relationship, id: Long, when: Date) = {
    rel("ts") = when.getTime
    rel("messageId") = id

    eventsIndex +=(rel, "type", ttype)
    eventsIndex +=(rel, "messageId", new ValueContext(id) indexNumeric())
    eventsIndex +=(rel, "ts", new ValueContext(when getTime) indexNumeric())
  }

  val QUERY = "type:%s AND messageId:%d"

  private def isNew(ttype: String, messageId: Long): Boolean = {
    val hits = eventsIndex.query(QUERY.format(ttype, messageId))
    val result = hits.getSingle
    hits.close()
    result == null
  }


  def saveRetweet(fromU: User, toU: User, thisMessageId: Long, baseMessageId: Long, when: Date) = {
    if (isNew("RT", thisMessageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val fromN: Node = indexUserInfo(fromU)
          val toN: Node = indexUserInfo(toU)
          val rel: Relationship = fromN --> "RT" --> toN <()
          saveEvent("RT", rel, thisMessageId, when)
          rel("baseMessageId") = baseMessageId
          eventsIndex +=(rel, "baseMessage", new ValueContext(baseMessageId) indexNumeric())
          println("Save Retweet user: " + fromU.getScreenName + " message: " + thisMessageId)
      }
    }
  }

  def saveMention(fromU: User, toId: Long, toScreenName: String, messageId: Long, when: Date) = {
    if (isNew("MENTION", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val fromN: Node = indexUserInfo(fromU)
          val toN: Node = getOrCreateUniqueUser(toId)
          if (toN("i").isEmpty) {
            toN("name") = toScreenName
            toN("i") = 1
            userIndex +=(toN, "name", toScreenName)
          }
          val rel: Relationship = fromN --> "MENTION" --> toN <()
          saveEvent("MENTION", rel, messageId, when)
          println("Save mention user " + fromU.getScreenName + " mention " + toScreenName)
      }
    }

  }

  def saveUrl(user: User, url: String, messageId: Long, when: Date) = {
    if (isNew("POSTED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val urlNode = getOrCreateUniqueEntity(url)
          val userNode = indexUserInfo(user)
          val rel: Relationship = userNode --> "POSTED" --> urlNode <()
          saveEvent("POSTED", rel, messageId, when)
          println("Save url user " + user.getScreenName + " posted " + url)
      }
    }
  }

  def saveUrl(username: String, url: String, messageId: Long, when: Date) = {
    if (isNew("POSTED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val userNode = getOrCreateUniqueUser(username)
          val urlNode = getOrCreateUniqueEntity(url)
          val rel: Relationship = userNode --> "POSTED" --> urlNode <()
          saveEvent("POSTED", rel, messageId, when)
          println("Save refinement url user " + username + " posted " + url)
      }
    }
  }

  def saveHashTag(user: User, hashTag: String, messageId: Long, when: Date) = {
    if (isNew("TAGGED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val userNode = indexUserInfo(user)
          val tagNode = getOrCreateUniqueEntity(hashTag)
          val rel: Relationship = userNode --> "TAGGED" --> tagNode <()
          saveEvent("TAGGED", rel, messageId, when)
          println("Save Hash tag user " + user.getScreenName + " posted " + hashTag)
      }
    }
  }

  def saveFriendship(from: Long, to: Seq[Long]) = {
    val fromN = getOrCreateUniqueUser(from)
    withTx {
      implicit ds: DatabaseService =>
        to foreach {
          friend: Long =>
            val toN = getOrCreateUniqueUser(friend)
            fromN --> "READS" --> toN
        }
        println("Save friendship")
    }
  }

  def batchNodesWithoutFriends(size: Int): List[Node] = {
    withTx {
      implicit db =>
        getAllNodes filterNot (n => n.hasRelationship(Direction.OUTGOING, DynamicRelationshipType.withName("READS"))) drop (1) take (size) toList
    }
  }

  def getUsersNotIndex(size: Int): List[Node] = {
    withTx {
      implicit db =>
        getAllNodes filter (n => !n.hasProperty("name") && n.hasProperty(USER_ID) && n.getProperty(USER_ID) != 1) drop (1) take (size) toList
    }
  }

}
