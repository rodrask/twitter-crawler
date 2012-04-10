package twitter.crawler
import twitter.crawler.metrics.Pattern
import collection.immutable.SortedSet

object Main extends App {
  val history = SortedSet[Long](1, 5, 10, 20, 50, 100, 1000)
  val borders = Pattern.makeBorders(1, List[Long](9,5,2)) //1, 10, 15, 17
  val a = new Pattern(borders)
  println(a.getProjectedHistory(history))
  println(a.getProjectedHistory(history, true))
//  var h: Long= -1
//  (1 to 1) foreach  {
//    _ =>
//      println("new Hop")
//      h = a.hopLength()
//      println(a.hopLength)
//      a.doHop(h)
//      println(a)
//      println(a.getProjectedHistory)
//  }
  println(List(1|1,1|0,0|1,0|0))
}
