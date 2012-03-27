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
import com.codahale.logula.Logging
import org.apache.lucene.document.Field.TermVector

object TweetStorage extends Actor with Logging {
  val MESSAGE_ID = "messageId"
  val analyzer: Analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val filedir = new java.io.File(storageProperties("lucene.storage"))
  val directory = new NIOFSDirectory(filedir)

  def conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);
  conf setOpenMode (IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
  conf.setMaxBufferedDocs(3000)
  val writer = new IndexWriter(directory, conf)

  def init = {
    val conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    conf setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    val writer = new IndexWriter(directory, conf)
    val doc = new Document
    doc.add(new Field("root", "root", Field.Store.YES, Field.Index.NOT_ANALYZED))
    writer.addDocument(doc)
    writer.close()
  }

  def saveTweet(status: Status): Unit = {
    if (!indexed(status.getId)) {
      val doc = toDocument(status.getId, status.getCreatedAt, status.getUser.getScreenName + " " + status.getText)
      index(doc)
    }
  }

  def saveTweet(status: Tweet): Unit = {
    {
      val doc = toDocument(status.getId, status.getCreatedAt, status.getFromUser + " " + status.getText)
      index(doc)
    }
  }

  def indexed(id: Long): Boolean = {
    val reader = IndexReader.open(writer, false)
    val query = new TermQuery(new Term(MESSAGE_ID, id.toString))
    val searcher = new IndexSearcher(reader)
    val h = searcher.search(query, 1).totalHits
    reader.close()
    h > 0
  }


  def index(doc: Document) = {
    writer.addDocument(doc)
    writer.commit

  }

  def toDocument(id: Long, date: Date, content: String): Document = {
    val doc = new Document
    doc.add(new Field(MESSAGE_ID, id.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(new Field("creationDate", dateToString(date, Resolution.SECOND), Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(new Field("content", content, Field.Store.NO, Field.Index.ANALYZED, TermVector.YES))
    return doc

  }

  def search(query: String): Seq[Document] = {
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
