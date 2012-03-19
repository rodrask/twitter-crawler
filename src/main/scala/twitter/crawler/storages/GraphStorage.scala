package twitter.crawler.storages

import org.neo4j.cypher.{CypherParser, ExecutionEngine}
import twitter.crawler.common.storageProperties
import com.codahale.logula.Logging
import scala.collection.JavaConversions._
import sys.ShutdownHookThread
import org.neo4j.scala.{DatabaseService, EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import twitter4j.User
import org.neo4j.graphdb.index.UniqueFactory
import java.util.{Map => JavaMap, Date}
import org.neo4j.index.lucene.ValueContext
import org.neo4j.graphdb.{DynamicRelationshipType, Direction, Relationship, Node}

object GraphStorage extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider with Logging {
  val USER_ID = "twId"
  val MESSAGE_ID = "messageId"
  val UNKNOWN = "unknown"

  override def neo4jStoreDir = storageProperties("graph.storage")

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

  def indexUserInfo(nodes: List[Node]) = {
    withTx {
      implicit ds: DatabaseService =>
        nodes foreach {
          userNode =>
            if (userNode("i").isEmpty) {
              userNode("name") = UNKNOWN
              userNode("i") = 1
              userIndex +=(userNode, "name", UNKNOWN)
              log.info("Save user's full info: neo_id = %d twId = %d   screenName = %s", userNode.getId, userNode.getProperty(USER_ID), userNode.getProperty("name"))
            }
        }
    }
  }

  def indexUserInfo(user: User): Node = {
    var userNode: Node = getOrCreateUniqueUser(user.getId)
    if (userNode("i").isEmpty) {
      withTx {
        implicit ds: DatabaseService =>
          userNode("name") = user.getScreenName
          userNode("creationDate") = user.getCreatedAt.getTime

          val location = if (user.getLocation == null) "" else user.getLocation
          val lang = if (user.getLang == null) "" else user.getLang
          val description = if (user.getDescription == null) "" else user.getDescription
          val profileBackgroundColor = if (user.getProfileBackgroundColor == null) "" else user.getProfileBackgroundColor
          val profileTextColor = if (user.getProfileTextColor == null) "" else user.getProfileTextColor
          val profileLinkColor = if (user.getProfileLinkColor == null) "" else user.getProfileLinkColor
          val profileSidebarFillColor = if (user.getProfileSidebarFillColor == null) "" else user.getProfileSidebarFillColor
          val profileSidebarBorderColor = if (user.getProfileSidebarBorderColor == null) "" else user.getProfileSidebarBorderColor

          userNode("location") = location
          userNode("lang") = lang
          userNode("description") = description
          userNode("followersCount") = user.getFollowersCount
          userNode("friendsCount") = user.getFriendsCount
          userNode("statusesCount") = user.getStatusesCount
          userNode("isProtected") = user.isProtected
          userNode("favouritesCount") = user.getFavouritesCount
          userNode("profileBackgroundColor") = profileBackgroundColor
          userNode("profileTextColor") = profileTextColor
          userNode("profileLinkColor") = profileLinkColor
          userNode("profileSidebarFillColor") = profileSidebarFillColor
          userNode("profileSidebarBorderColor") = profileSidebarBorderColor
          userNode("profileUseBackgroundImage") = user.isProfileUseBackgroundImage


          userNode("i") = 1

          userIndex +=(userNode, "name", user.getScreenName)
          userIndex +=(userNode, "creationDate", new ValueContext(user.getCreatedAt.getTime) indexNumeric)
          log.info("Save user's full info: neo_id = %d twId = %d   screenName = %s", userNode.getId, userNode.getProperty(USER_ID), userNode.getProperty("name"))
      }
    }
    userNode
  }

  private def saveEvent(ttype: String, rel: Relationship, id: Long, when: Date) = {
    rel("ts") = when.getTime
    rel(MESSAGE_ID) = id

    eventsIndex +=(rel, "type", ttype)
    eventsIndex +=(rel, MESSAGE_ID, new ValueContext(id) indexNumeric())
    eventsIndex +=(rel, "ts", new ValueContext(when getTime) indexNumeric())
  }

  val QUERY = "type:%s AND " + MESSAGE_ID + " :%d"

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
          log.info("Save Retweet by user: %s from message %d in %d", fromU.getScreenName, baseMessageId, thisMessageId)
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
          log.info("Save mention: user %s mentions %s in %d", fromU.getScreenName, toScreenName, messageId)
      }
    }

  }

  def saveUrl(user: User, showedUrl: String, realUrl: String, messageId: Long, when: Date) = {
    if (isNew("POSTED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val urlNode = getOrCreateUniqueEntity(realUrl)
          val userNode = indexUserInfo(user)
          val rel: Relationship = userNode --> "POSTED" --> urlNode <()
          rel("showedUrl") = showedUrl
          eventsIndex +=(rel, "showedUrl", showedUrl)
          saveEvent("POSTED", rel, messageId, when)
          log.info("Save url: user %s posted %s in %d ", user.getScreenName, realUrl, messageId)
      }
    }
  }

  def saveUrlFromSearch(username: String, realUrl: String, messageId: Long, when: Date) = {
    if (isNew("POSTED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val userNode = getOrCreateUniqueUser(username)
          val urlNode = getOrCreateUniqueEntity(realUrl)
          val rel: Relationship = userNode --> "POSTED" --> urlNode <()
          saveEvent("POSTED", rel, messageId, when)
          log.info("Save refinement url: user %s posted %s in %d", username, realUrl, messageId)
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
          log.info("Save Hash tag: user %s posted %s in %d", user.getScreenName, hashTag, messageId)
      }
    }
  }

  def saveFriendship(from: Long, to: Seq[Long], countUser: Int): Int = {
    val fromN = getOrCreateUniqueUser(from)
    var newCountUser = countUser
    withTx {
      implicit ds: DatabaseService =>
        to foreach {
          friend: Long =>
            val toN = getOrCreateUniqueUser(friend)
            fromN --> "READS" --> toN
            log.info("Save friendship: user %d reads %d", fromN.getId, toN.getId)
            newCountUser += 1
        }
    }
    newCountUser
  }

  /*
  * Ниже буду аналитические функции

  */

  val cypherParser = new CypherParser
  val engine = new ExecutionEngine(ds.gds);

  def foreverAloneUsers()={
    val r= engine.execute(
      """
      start user=node:users("name:*")
      match user-[:TAGGED]->url
      return url, count(user) order by count(user)""")
    println(r.dumpToString)

  def partialUsers(): Seq[Node] = {
    Nil
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
