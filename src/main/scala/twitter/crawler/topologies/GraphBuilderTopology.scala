package twitter.crawler.topologies

import twitter.crawler.storages.{TweetStorage, FutureTasksStorage}
import twitter.crawler.storm.TwitterStreamListener
import twitter.crawler.common.TwitterService
import twitter.crawler.threads.{SearchThread, RetweetsThread}
import twitter4j.FilterQuery

object GraphBuilderTopology {
  def main(args: Array[String]) = {
    
    val runTimeHours:Int = args(0).toInt
    TweetStorage.start()
    FutureTasksStorage.start()
    RetweetsThread.start()
    SearchThread.start()
    val tStream = TwitterService.newStreamInstance()
    tStream.addListener(new TwitterStreamListener)
    val filterQuery = new FilterQuery
    tStream.sample()
    
    Thread sleep  1000 * 60 * 60 * runTimeHours

    RetweetsThread.interrupt
    SearchThread.interrupt()
    tStream.shutdown()

    FutureTasksStorage ! 'stop
    TweetStorage ! 'stop
  }

}