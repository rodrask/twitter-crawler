package ru.hse.twitter.analytics

import org.neo4j.tooling.GlobalGraphOperations
import collection.JavaConversions._
import collection.mutable.Map;
import org.neo4j.graphdb.{Direction, Node}
import scala.util.Sorting.stableSort
import twitter.crawler.storages.DBStorage

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 03.02.12
 * Time: 1:03
 * To change this template use File | Settings | File Templates.
 */

object SocialGraphProperties {
  
  def topUsers ={
    val gds = DBStorage.storage.ds.gds
    val ggo = GlobalGraphOperations.at(gds)
    val result = stableSort( ggo.getAllNodes map (n => ( countRelations(n, Direction.OUTGOING), n.getProperty("twId", 0l).asInstanceOf[Long])) toList)
    result
  }

  def friendsNumberDistribution:Map[Int, Long] = {
    val result:Map[Int, Long] = Map[Int, Long]()
    val gds = DBStorage.storage.ds.gds
    val transaction = gds.beginTx()
    val ggo = GlobalGraphOperations.at(gds)
    for (c <- ggo.getAllNodes map (n => countRelations(n, Direction.INCOMING)))
    {
      println(c)
      if (result.containsKey(c))
        result(c) += 1
      else result += (c -> 1l)
    }
    transaction.success()
    result
  }
  private def countRelations(node: Node, direction: Direction=Direction.INCOMING): Int={
    if (! node.hasRelationship(direction))
      0
    else{
      node.getRelationships(direction).size
    }
  }

}