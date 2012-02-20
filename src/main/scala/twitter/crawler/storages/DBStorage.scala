package twitter.crawler.storages
import sys.ShutdownHookThread
import org.neo4j.graphdb.index.Index
import org.neo4j.scala._
import twitter4j.User
import twitter.crawler.common.properties
import collection.JavaConversions._
import org.neo4j.graphdb.{Relationship, DynamicRelationshipType, Direction, Node}

case class UserInfo(users: List[User])

case class UserFriends(userId: Long, friends: List[Long])

case class NodeInfo(name: String)

case class UpdateError(ex: Exception)

class DBStorage(path: String = "tmp/twitter-graph") extends Neo4jWrapper with Neo4jIndexProvider with EmbeddedGraphDatabaseServiceProvider {
  override def neo4jStoreDir = path
  override def NodeIndexConfig = ("users", Map("provider" -> "lucene", "type" -> "exact")) :: Nil

  ShutdownHookThread {
    shutdown(ds)
  }

  private def userIndex(implicit db: DatabaseService): Index[Node] = getNodeIndex("users").get

  private def getUserNode(id: Long)(implicit db: DatabaseService): Option[Node] = {
    val hits = userIndex.get("twId", id.toString)
    val user = hits.getSingle
    hits.close()
    if (user != null) Some(user) else None
  }

  def batchDummyNodes(size: Int): List[Long] = {
    withTx {
      implicit db =>
        val hits = userIndex.query("name", DBStorage.DUMMY)
        val result = asScalaIterator(hits).take(size).toList
        hits.close()
        result map {
          n => n[Long]("twId").get
        }
    }
  }

 def batchNodesWithoutFriends(size: Int): List[Node] = {
    withTx {
      implicit db =>
        getAllNodes filterNot (n => n.hasRelationship(Direction.OUTGOING, DynamicRelationshipType.withName("READS"))) drop (1) take(size) toList
    }
  }

  private def initNode(id: Long)(implicit db: DatabaseService): Node = {
    val node = createNode
    node("twId") = id
    userIndex +=(node, "twId", id.toString)
    userIndex +=(node, "name", DBStorage.DUMMY)
    node
  }

  private def fullIndex(user: User)(implicit db: DatabaseService) = {
    val index = userIndex
    val node = getUserNode(user.getId) match {
      case None =>
        createNode
      case Some(n) =>
        index -=(n, "name")
        n
    }

    val location = if (user.getLocation == null) "" else user.getLocation
    val lang = if (user.getLang == null) "" else user.getLang

    node("twId") = user.getId
    node("name") = user.getScreenName
    node("location") = location
    node("creationDate") = user.getCreatedAt.getTime
    node("lang") = lang

    index +=(node, "twId", user.getId.toString)
    index +=(node, "name", user.getScreenName)
    index +=(node, "location", location)
    index +=(node, "creationDate", user.getCreatedAt.getTime.toString)
    index +=(node, "lang", lang)

    println("User " + user + " succesfully added")
  }

  def batchIndex(users: List[User]) = {
    withTx {
      implicit db: DatabaseService =>
        users foreach {
          u => fullIndex(u)
        }
    }
  }


  private def getFriendNode(friendId: Long)(implicit db: DatabaseService): Node = {
    getUserNode(friendId) match {
      case None =>
        initNode(friendId)
      case Some(node) =>
        node
    }
  }

  def insertFriends(userId: Long, friends: List[Long]): Unit = {
    withTx {
      implicit db: DatabaseService =>
        val user = getUserNode(userId) match {
          case None =>
            initNode(userId)
          case Some(node) =>
            node
        }
        friends foreach {
          f =>
            val friendNode = getFriendNode(f)
            user --> "READS" --> friendNode
        }
    }
  }

  def nodeInfo(id: Long) = {
    withTx {
      implicit db =>
        val h = getUserNode(id).get
        println("name " + h("name"))
        println("id " + h("twId"))
        println(h.hasRelationship)
        for (r: Relationship <- h.getRelationships) {
          println(r.getStartNode()("name") + " --> " + r.getEndNode()("name"))
        }
    }
  }

  def nodeInfo(name: String) = {
    println(name)
    withTx {
      implicit db =>
        val hits = userIndex.query("name", name)
        for (h: Node <- hits.iterator()) {
          println("name " + h("name"))
          println("id " + h("twId"))
          println(h.hasRelationship)
          for (r: Relationship <- h.getRelationships(Direction.OUTGOING)) {
            println(r.getStartNode()("name") + " --> " + r.getEndNode()("name"))
          }
        }
    }

  }

  //  def act() {
  //    loop {
  //      react {
  //        case UserInfo(result: List[User]) =>
  //          try {
  //            batchIndex(result)
  //            println("Insering user data finished")
  //          } catch {
  //            case ex: Exception => println("Exception when save user info:\n " + ex.getMessage)
  //            ex.printStackTrace()
  //          }
  //        case UserFriends(userId: Long, friends: List[Long]) =>
  //          try {
  //            insertFriends(userId, friends)
  //            println("Insering friends")
  //          } catch {
  //            case ex: Exception => println("Exception when save user info:\n " + ex.getMessage)
  //            ex.printStackTrace()
  //          }
  //        case NodeInfo(name: String) =>
  //          nodeInfo(name)
  //
  //      }
  //    }
  //  }
}

object DBStorage {
  val DUMMY = "@DUMMYNAME@"
  val storage: DBStorage = new DBStorage(properties("graphdb.path"))
  //  storage.start
}
