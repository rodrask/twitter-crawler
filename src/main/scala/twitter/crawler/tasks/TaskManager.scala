package twitter.crawler.tasks

import java.util.Date
import scala.collection.mutable.Map
import scala.util.Random
import actors.{Exit, Actor}
import twitter4j.{TwitterException, TwitterFactory, TwitterStreamFactory}
import actors.scheduler.DaemonScheduler

case class TaskInfo(task: Task, properties: Map[String, Any])

object TaskManager extends Actor {
  override def scheduler = DaemonScheduler
  this.trapExit = true
  val rand: Random = new Random(7363874l)
  var tasks: Map[Long, TaskInfo] = Map()
  var runningTask: Int = 0;
  def decrement = {
    runningTask -= 1
    if (runningTask < 0 )
      this ! 'StopApplication
  }
  this.start()

  def attachTask(task: Task, allowedCalls: Int): Long = {
    val taskId = rand.nextLong()
    tasks += (taskId -> TaskInfo(task, Map("calls" -> allowedCalls)))
    task.twLimits = allowedCalls
    taskId
  }


  def extractTask(id: Long): Task = {
    val TaskInfo(task, _) = tasks(id)
    task
  }

  def act() = {
    //tasks.values foreach {ti => link(ti.task)}
    loop {
      react {
        case StartTask(id) =>
          extractTask(id).start() ! Begin
          runningTask += 1
        case StopTask(id) =>
          extractTask(id) ! Stop
          decrement
        case PauseTask(id) =>
          extractTask(id) ! Pause
        case ResumeTask(id) =>
          extractTask(id).notify()
        case Exit(from, ex) =>
          ex match {
            case 'RateLimitsForTaskReached =>
              println("Task "+from+" uses all it calls")
              from ! Stop
              decrement
            case 'RestRateLimitsExceeds =>
              println("RateLimits reached. Stop all tasks")
              tasks foreach { case (taskId: Long, ti: TaskInfo) => ti.task ! Stop;decrement }
            case _ =>
              println("Unknown error: "+ex+". Restart task")
              link(from)
              from.asInstanceOf[Task].restart()
              from.asInstanceOf[Task] ! Begin
          }
        case 'StopApplication =>
          println("Exit command received")
          tasks foreach { case (taskId: Long, ti: TaskInfo) => ti.task ! Stop }
          exit("Stop application")
      }
    }
  }

}

case class StartTask(id: Long)

case class StopTask(id: Long)

case class PauseTask(id: Long)

case class ResumeTask(id: Long)


