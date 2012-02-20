package twitter.crawler.rest

import actors.Actor
import twitter4j.TwitterException
import actors.scheduler.DaemonScheduler
import twitter.crawler.tasks.Stop

/**
 * Created by IntelliJ IDEA.
 * User: bernx
 * Date: 26.01.12
 * Time: 0:43
 * To change this template use File | Settings | File Templates.
 */
case class RestRequest[T](data: T)
case class RestException(ex: Exception)
class RestActor[In, Out](method: In => Out, store: Out => Unit) extends Actor {
  override def scheduler = DaemonScheduler
  def act() {
    loopWhile(true){
      react {
        case RestRequest(data: In) =>
          store(method(data))
        case Stop =>
          exit("Actor " + toString + " exits")
      }
    }
  }
  override def exceptionHandler = {
    case ex: TwitterException =>
      println(ex.getExceptionCode+ " "+ ex.getMessage)
      if (ex.exceededRateLimitation()){
        sender ! 'RestRateLimitsExceeds
      }
      else
        sender ! RestException(ex)

    case ex: Exception =>
      println("Exception: "+ex.getMessage)
  }
}

