package twitter.crawler.storages

import actors.Actor
import twitter4j._
import java.util.Date
import org.apache.lucene.index._
import org.apache.lucene.store._
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search._
import org.apache.lucene.document._
import twitter.crawler.common.storageProperties
import org.apache.lucene.document.DateTools.{Resolution, dateToString}

object TweetStorage extends Actor {
  val MESSAGE_ID = "messageId"
	val analyzer: Analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val filedir = new java.io.File(storageProperties("lucene.storage"))
  val directory = new NIOFSDirectory(filedir)

  if (!filedir.exists) { filedir.mkdir }

  val conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);
  conf setOpenMode (IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

  var reader = IndexReader.open(directory, true)


  def saveTweet(status: Status): Unit = {
    if (! indexed(status.getId))
    {
      val doc = toDocument(status.getId, status.getCreatedAt, status.getUser.getScreenName+" "+status.getText)
      index(doc)
    }
  }

  def saveTweet(status: Tweet): Unit = {
    {
      val doc = toDocument(status.getId, status.getCreatedAt, status.getFromUser+" "+status.getText)
      index(doc)
    }
  }

  def indexed(id: Long): Boolean = {
    reader = IndexReader.openIfChanged(reader, true)
    val query = new TermQuery(new Term(MESSAGE_ID, id.toString))
    val searcher = new IndexSearcher(reader)
    searcher.search(query, 1).totalHits > 0
  }

  def index(doc: Document)={
    val writer = new IndexWriter(directory, conf)
    writer.addDocument(doc)
    writer close
  }

  def toDocument(id: Long, date: Date, content: String): Document={
    val doc = new Document
    doc.add(new Field(MESSAGE_ID, id.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(new Field("creationDate", dateToString(date, Resolution.SECOND) , Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(new Field("content", content, Field.Store.NO, Field.Index.ANALYZED))
    return doc

  }

  def search(qury: String): Seq[Document]={
    return List[Document]()
  }


def act() {
    loopWhile(true) {
      react {
        case ('index, status: Status) =>
          saveTweet(status)
        case ('index, status: Tweet) =>
          saveTweet(status)
        case ('search, query: String) =>
          sender ! search(query)
        case 'stop =>
          exit
      }
    }
  }

}
