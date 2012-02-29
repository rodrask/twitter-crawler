package twitter.crawler.storages

import twitter4j.Status
import org.apache.lucene.index._
import org.apache.lucene.store._
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search._
import org.apache.lucene.document._

object TweetStorage {

	val analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val filedir = new java.io.File("tweets_storage")
  val directory = new NIOFSDirectory(filedir)

  if (!filedir.exists) { filedir.mkdir }
	val writer = new IndexWriter(directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED)



  def saveTweet(status: Status) = {



  }



  def indexed(status: Status) = {
    true
  }

}
