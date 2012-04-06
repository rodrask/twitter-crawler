package twitter.crawler

import storages.GraphStorage
import java.util.{Date, Calendar}
import java.io.FileWriter

object Main extends App {
	val file = new FileWriter("result.txt", true)
	println("before")
  GraphStorage.makeQuery(
  """
  start u=node:users("name:navalny")
  match ()-[r:READS]->u
  return u.twId? as id, u.name? as name, count(r) as n limit 100
  """
  ) foreach {m => println(m("n"))}

  	GraphStorage.stop
  	file.close()
  println("exit")
  0
}
