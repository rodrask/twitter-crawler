package twitter.crawler.topologies

import twitter.crawler.threads.{RedisSearchThread, RedisRetweetsThread}
import twitter.crawler.storages.{GraphStorage, TweetStorage}

object SeachUrlOnlyTopology {

  def init() = {

    TweetStorage.start()
    GraphStorage.start()
    RedisSearchThread.withRemoving = true
    RedisSearchThread.start()
  }

  def stop = {
    RedisSearchThread.interrupt()
    TweetStorage ! 'stop
    GraphStorage ! 'stop
  }

  def main(args: Array[String]) = {
    init
    var command = ""
    while (!"exit".equals(command)) {
      command = Console.readLine()
    }
    stop
  }

}