package twitter.crawler.storages

import twitter.crawler.common.properties
import sys.ShutdownHookThread
import org.neo4j.scala.{DatabaseService, EmbeddedGraphDatabaseServiceProvider, Neo4jIndexProvider, Neo4jWrapper}
import java.util.Date
import twitter4j.User
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.graphdb.index.Index

object GraphStorage extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {
  type UserId = Either[Long, String]

  override def neo4jStoreDir = properties("graphdb.path")

  val indexProperties: IndexCustomConfig = Some(Map("provider" -> "lucene", "type" -> "exact"))

  override def NodeIndexConfig = ("users", indexProperties) ::("urls", indexProperties) ::
    ("hashtags", indexProperties)::Nil

  override def RelationIndexConfig =
    ("retweets", indexProperties) ::
    ("urls", indexProperties) ::
    ("mentions", indexProperties) ::
    ("hashtags", indexProperties)  ::Nil

  def userIndex = getNodeIndex("user").get
  def urlIndex = getNodeIndex("urls").get
  def tagIndex = getNodeIndex("hashtags").get

  def retweetsIndex = getRelationIndex("retweets").get
  def urlMentionIndex = getRelationIndex("urls").get
  def mentionIndex = getRelationIndex("mentions").get
  def tagMentionIndex = getRelationIndex("hashtags").get


  ShutdownHookThread {
    shutdown(ds)
  }

  
  private def getNode[T](index: Index[Node], field: String)(value: T): Option[Node]={
    val hits = index.get(field, value.toString)
    val result = hits.getSingle
    hits.close()
    if (result != null) Some(result) else None
  }

  private def getUrl = getNode(urlIndex, "url")
  private def getHashTag = getNode(tagIndex, "tag")
  def getUserById = getNode[Long](userIndex, "twId")
  def getUserByName = getNode[String](userIndex, "name")

  def insertUser(user: User)(implicit ds: DatabaseService) = {
    val userNode = getUserById(user.getId) match {
      case None =>
        insertStubUser(Left(user.getId))
      case Some(node) =>
        node
    }

    userNode("twId") = user.getId
    userIndex +=(userNode, "twId", user.getId.toString)

    userNode("name") = user.getScreenName
    userIndex +=(userNode, "name", user.getScreenName)

    val location = if (user.getLocation == null) "" else user.getLocation
    userNode("location") = location
    userIndex +=(userNode, "location", location)

    userNode("creationDate") = user.getCreatedAt.getTime
    userIndex +=(userNode, "creationDate", user.getCreatedAt.getTime.toString)

    val lang = if (user.getLang == null) "" else user.getLang
    userNode("lang") = lang
    userIndex +=(userNode, "lang", lang)
  }


  def insertStubUser(userId: UserId): Node = {
    val node = createNode
    if (userId.isLeft) {
      node("twId") = userId.left
      userIndex +=(node, "twId", userId.left.toString)
    }
    else {
      node("name") = userId.right
      userIndex +=(node, "name", userId.right)

    }
    node
  }

  def getOrInsertUser(userName: String): Node = getUserByName(userName) match {
    case Some(node) => node
    case None => insertStubUser(Right(userName))
  }

  def getOrInsertUrl(url: String): Node = getUrl(url) match {
    case Some(node) => node
    case None => 
      val result = createNode
      result("url") = url
      urlIndex += (result, "url", url)
      return result
  }
  
  def getOrInsertHashTag(hashTag: String): Node = getHashTag(hashTag) match {
    case Some(node) => node
    case None =>
      val result = createNode
      result("hashtag") = hashTag
      tagIndex += (result, "tag", hashTag)
      return result
  }

  case class CommonRelProperties(timestamp: Long, messageId: Long)
  
  def saveRetweet(fromUser: String, toUser: String, thisMessageId: Long, baseMessageId:Long, date: Date)={
    withTx {
      implicit ds: DatabaseService =>
        val from: Node = getOrInsertUser(fromUser)
        val to: Node = getOrInsertUser(toUser)
        val rel: Relationship = from --> "RETWEET" --> to <()

        rel("ts") = date.getTime
        rel("mId") = thisMessageId
        rel("baseMessageId") = baseMessageId
    }
  }
  def saveMention(fromUser: String, toUser: String, ts: Date, messageId: Long) = {
    withTx {
      implicit ds: DatabaseService =>
        val from: Node = getOrInsertUser(fromUser)
        val to: Node = getOrInsertUser(toUser)
        val rel: Relationship = from --> "MENTION" --> to <()
        
        rel("ts") = ts.getTime
        rel("mId") = messageId
    }

  }

  def saveUrlPost(user: String, url: String, when: Date, messageId: Long) = {
    withTx {
      implicit ds: DatabaseService =>
        val userNode = getOrInsertUser(user)
        val urlNode = getOrInsertUrl(url)
        val rel: Relationship = userNode --> "POSTED" --> urlNode <()

        rel("ts") = when.getTime
        rel("mId") = messageId
      
    }

  }

  def saveHashTag(user: String, hashTag: String, when: Date, messageId: Long) = {
    withTx {
      implicit ds: DatabaseService =>
        val userNode = getOrInsertUser(user)
        val tagNode = getOrInsertTag(user)
        val rel: Relationship = userNode --> "TAGGED" --> tagNode <()

        rel("ts") = when.getTime
        rel("mId") = messageId
        indexRelation()

    }

  }

  def saveHashTagUsage(hashTag: String, when: Date) = {

  }

  def saveUrlUsage(url: String, when: Date) = {

  }


}
