package twitter.crawler.tasks

import collection.JavaConversions._
import twitter4j.User
import twitter.crawler.common._
import twitter.crawler.storages.DBStorage
import twitter.crawler.rest.{RestRequest, RestActor}


class SeedTask(filePath: String = "twi_top100.txt") extends Task {
  val usernames = loadNames("twi_top100.txt").toArray
  override var twLimits = 350
  val lookupUsers = new RestActor[Array[String], List[User]](
  { input: Array[String] => twitter.lookupUsers(input).toList },
  {
    result: List[User] =>
      DBStorage.storage.batchIndex(result)
  })

  def act() ={
    link(lookupUsers)
    loop{
      react{
        case Begin =>
          lookupUsers.start()
          lookupUsers ! RestRequest[Array[String]](usernames)
        case Pause =>
          println("Task "+ toString +" paused")
          this.wait()
        case Stop =>
          exit()
      }
    }


  }
}