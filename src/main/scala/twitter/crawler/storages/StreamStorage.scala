package twitter.crawler.storages

import java.io.FileWriter
import java.text.SimpleDateFormat
import twitter4j.Status
import java.util.Date
import twitter.crawler.common.extractURL

object StreamStorage {
  val commonFile = "tweets.txt"
  val urlsFile = "urls.txt"
  val retweetsFile = "retweets.txt"

  val urlFileWriter: FileWriter = new FileWriter(urlsFile)
  val commonFileWriter: FileWriter = new FileWriter(commonFile)


  val tweetFormat = "%d\t%d\t%s\t%s\t%s\t%s\t%s\n"
  val dateFormat = new SimpleDateFormat("dd.MM.yyyy/HH:mm")

  val uRecFormat = "%d\t%d\t%s\t%s\n"
  def saveUrlRecord(twId: Long, userId: Long, timestamp: Date, url: String) = {
    urlFileWriter.write(uRecFormat.format(twId, userId, dateFormat.format(timestamp), url))
  }
  def saveTweet(status: Status) = {
    val urls = buildString(status.getURLEntities map (ue => extractURL(ue)))
    val hashTags = buildString(status.getHashtagEntities map (ht => ht.getText))
    val mentions = buildString(status.getUserMentionEntities map (m => m.getScreenName))
    val dateStr = dateFormat.format(status.getCreatedAt)
    commonFileWriter.write(tweetFormat.format(status.getId, status.getUser.getId, dateStr, status.getText, urls, hashTags, mentions))
  }

  private def buildString(data: Seq[String]): String = {
    if (data.isEmpty)
      "<NoEntities>"
    else
      data mkString("<", "|", ">")
  }


  def end() = {
    println("close file")
    commonFileWriter.close()
    urlFileWriter.close()
  }

}
