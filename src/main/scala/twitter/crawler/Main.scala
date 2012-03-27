package twitter.crawler

import storages.GraphStorage
import java.util.{Date, Calendar}
import java.io.FileWriter

object Main extends App {
  val d = GraphStorage.topFriendsUsers()
  val file = new FileWriter("uids.txt", true)
  d foreach {u:Long => file.write(u.toString);file.write("\n")}

}
