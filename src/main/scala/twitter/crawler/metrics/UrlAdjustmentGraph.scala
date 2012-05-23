package twitter.crawler.metrics
import twitter.crawler.metrics.JoinedProcesses
import collection.immutable.SortedSet

class UrlAdjustmentGraph {

}

object UrlAdjustmentGraph{
  def buildGraph(queryResult: Map[String, List[Long]]): List[(String, String, Double)]={
    val users:SortedSet[String] = SortedSet[String](queryResult.keys.toList: _*)
    var result:List[(String, String, Double)] = List()
    for (ext_user <- users){
      e_Points
      for (int_user
     <- users.from(ext_user)){

         JoinedProcesses.calculateIT(SortedSet)
    }}
  }
}
