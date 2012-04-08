package twitter.crawler.topologies

import twitter.crawler.storm.TwitterStreamListener
import twitter.crawler.common.TwitterService
import twitter4j.FilterQuery
import twitter.crawler.common.loadId
import twitter.crawler.threads.{RedisSearchThread, RedisRetweetsThread, SearchThread, RetweetsThread}
import twitter.crawler.storages.{GraphStorage, TweetStorage, FutureTasksStorage}

object RedisGraphBuilderTopology {

  def init() = {
    TweetStorage.start()
    GraphStorage.start()

    RedisRetweetsThread.start()
    RedisSearchThread.start()

  }

  def stop={
    RedisSearchThread.interrupt()
    RedisRetweetsThread.interrupt()

    TweetStorage ! 'stop
    GraphStorage ! 'stop
  }

  def createFilter = {
    val ids = loadId("ids.txt")
    val filterQuery = new FilterQuery
    filterQuery.follow(ids.toArray)
    filterQuery
  }

  def main(args: Array[String]) = {
    init
    val tStream = TwitterService.newStreamInstance()
    tStream.addListener(new TwitterStreamListener)
    tStream.filter(createFilter)

    var command = ""
    while (!"exit".equals(command)) {
      command = Console.readLine()
    }
   tStream.shutdown()
    stop
  }

}