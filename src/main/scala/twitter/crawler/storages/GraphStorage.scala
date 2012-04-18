package twitter.crawler.storages

import org.neo4j.cypher.{CypherParser, ExecutionEngine}
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
import twitter4j.{Tweet, Status, User}
import collection.immutable.SortedSet

object GraphStorage extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider with NeoQueriesTrait with Logging with Actor {
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

  def stopStorage = {
    exit()
    shutdown(ds)
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

  private def saveRetweet(fromU: User, toU: User, thisMessageId: Long, baseMessageId: Long, when: Date) = {
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

  private def saveMention(fromU: User, toId: Long, toScreenName: String, messageId: Long, when: Date) = {
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

  private def saveUrl(user: User, showedUrl: String, realUrl: String, messageId: Long, when: Date) = {
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

  private def saveUrlFromSearch(username: String, userId: Long, realUrl: String, messageId: Long, when: Date) = {
    if (isNew("POSTED", messageId)) {
      withTx {
        implicit ds: DatabaseService =>
          val userNode = getOrCreateUniqueUser(userId)
          userNode("name") = username
          userIndex +=(userNode, "name", username)
          val urlNode = getOrCreateUniqueEntity(realUrl)
          val rel: Relationship = userNode --> "POSTED" --> urlNode <()
          saveEvent("POSTED", rel, messageId, when)
          log.info("Save refinement url: user %s posted %s in %d", username, realUrl, messageId)
      }
    }
  }

  private def saveHashTag(user: User, hashTag: String, messageId: Long, when: Date) = {
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

  def act() {
    loopWhile(true) {
      react {
        case ('save_rt, fromU: User, toU: User, thisMessageId: Long, baseMessageId: Long, when: Date) =>
          saveRetweet(fromU, toU, thisMessageId, baseMessageId, when)
        case ('save_mention, fromU: User, toId: Long, toScreenName: String, messageId: Long, when: Date) =>
          saveMention(fromU, toId, toScreenName, messageId, when)
        case ('save_url, user: User, showedUrl: String, realUrl: String, messageId: Long, when: Date) =>
          saveUrl(user, showedUrl, realUrl, messageId, when)
        case ('save_rf_url, username: String, userId: Long, realUrl: String, messageId: Long, when: Date) =>
          saveUrlFromSearch(username, userId, realUrl, messageId, when)
        case ('save_hash, user: User, hashTag: String, messageId: Long, when: Date) =>
          saveHashTag(user, hashTag, messageId, when)
        case 'stop =>
          stopStorage
      }
    }
  }

  /*
  * Ниже буду аналитические функции

  */
  // def makeQuery(query: String) = {
  //   engine.execute(query)
  // }

  // val cypherParser = new CypherParser
  // val engine = new ExecutionEngine(ds.gds);

  // def topFriendsUsers(topN: Int = 100) = {
  //   val r = engine.execute(
  //     """
  //     start user=node:users("name:*")
  //     match user-[r:READS]->f
  //     return user.twId as id, user.name as user, count(f) as n order by count(f) desc limit {topN}
  //     """, Map("topN" -> topN))
  //   r map (m => m("id"))
  // }

  // def usersWithUrls(from: Date, to: Date) = {
  //   val r = engine.execute(
  //     """
  //     start user=node:users(nodeType="USER")
  //     match user-[r:POSTED]->url
  //     where r.ts > {from} and r.ts < {to}
  //     return user.name as user, collect(url) as urls
  //     """, Map("from" -> from.getTime, "to" -> to.getTime))
  //   r foreach {
  //     m => println(m)
  //   }
  // }

  // def aloneUsers() = {
  //   val r = engine.execute(
  //     """
  //     start user=node:users(nodeType="USER")
  //     match user-[:POSTED]->url
  //     return user.name?, collect(url.name?)  LIMIT 100
  //     """)
  //   r
  // }

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

  def getUsername(userId: Long): String={
    val hits = userIndex.get(USER_ID, new ValueContext(userId).getValue)
      if (hits.size() > 0)
        hits.getSingle.getProperty("name", "None").toString
      else
        "Bad userId: "+userId
  }

  def userUrlsTs(userId: Long): SortedSet[Long] = {
    val node = userIndex.get(USER_ID, new ValueContext(userId).getValue).getSingle
    if (node == null){
      log.info("Bad id: %d",userId)
      return SortedSet.empty
    }
    log.info("Urls for user: %s",node("name").getOrElse("None"))
    val extractFunc: Relationship => Long = {
      rel =>
        rel[Long]("ts").get / 1000
    }
    SortedSet(node.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("POSTED")).toList.map(extractFunc): _*)
  }
  def userUrls(userId: Long): Seq[(String, Long)] = {
    val node = userIndex.get(USER_ID, new ValueContext(userId).getValue).getSingle
    log.info("Urls for user: %s",node("name"))
    val extractFunc: Relationship => (String, Long) = {
      rel =>
        (rel.getEndNode().getProperty("name", "None").toString, rel[Long]("ts").get)
    }
    node.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("POSTED")).toList.map(extractFunc)
  }

  def userRetweeters(userId: Long): Set[Long] = {
    val node = userIndex.get(USER_ID, new ValueContext(userId).getValue).getSingle
    if (node == null){
      log.info("Bad id: %d",userId)
      return Set.empty
    }
    val extractFunc: Relationship => Long = {
      rel =>
        rel.getStartNode.getProperty("twId").asInstanceOf[Long]
    }
    Set(node.getRelationships(Direction.INCOMING, DynamicRelationshipType.withName("RT")).toList.map(extractFunc): _*)
  }
}
