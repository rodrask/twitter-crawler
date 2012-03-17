package twitter.crawler.topologies

import twitter.crawler.storages.{TweetStorage, FutureTasksStorage}
import twitter.crawler.storm.TwitterStreamListener
import twitter.crawler.common.TwitterService
import twitter.crawler.threads.{SearchThread, RetweetsThread}

object GraphBuilderTopology {
  def main(args: Array[String]) = {
    TweetStorage.start()
    FutureTasksStorage.start()
    RetweetsThread.start()
    SearchThread.start()
    val tStream = TwitterService.newStreamInstance()
    tStream.addListener(new TwitterStreamListener)
    tStream.sample()
    
    Thread sleep  60*1000

    RetweetsThread.interrupt
    SearchThread.interrupt()
    tStream.shutdown()

    FutureTasksStorage ! 'stop
    TweetStorage ! 'stop
  }

}