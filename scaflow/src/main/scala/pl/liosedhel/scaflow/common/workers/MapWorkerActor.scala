package pl.liosedhel.scaflow.common.workers

import akka.actor._
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Master.{ WorkDone, GiveMeWorkToDo, WorkerCreated }
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Worker.{ NoMoreWorkToDo, WorkCompleted, WorkIsReady }
import pl.liosedhel.scaflow.common.model.NextVal
import pl.liosedhel.scaflow.common.workers.MapWorkerActor.{ Working, WaitingForJob, Idle, WorkerState }

import scala.concurrent.Future
import scala.util.{ Failure, Success }

object MapWorkerActor {

  sealed trait WorkerState
  case object Idle extends WorkerState
  case object WaitingForJob extends WorkerState
  case object Working extends WorkerState

  def props[A, B](publisher: ActorRef, operation: A => B) = {
    Props(new MapWorkerActor[A, B](publisher, operation))
  }
}

class MapWorkerActor[A, B](master: ActorRef,
  operation: A => B)
    extends FSM[WorkerState, Option[NextVal[A]]] with ActorLogging {
  implicit val ec = context.dispatcher

  master ! WorkerCreated(self)

  startWith(Idle, None)

  when(Idle) {
    case Event(WorkIsReady(), _) => {
      master ! GiveMeWorkToDo()
      goto(WaitingForJob) using None
    }
  }

  when(WaitingForJob) {
    case Event(n: NextVal[A], _) =>
      Future {
        operation(n.data)
      } onComplete {
        case Success(result) => self ! WorkCompleted(result)
        case f @ Failure(_) => self ! f
      }
      goto(Working) using Some(n)
    case Event(NoMoreWorkToDo(), _) => goto(Idle) using None
    case Event(WorkIsReady(), _) => stay() using None //ignore event - cause async message passing worker while waiting for job can be notified about new job
  }

  when(Working) {
    case Event(WorkCompleted(result), jobOpt) => {
      val job = jobOpt.get
      master ! WorkDone(job, result)
      goto(Idle) using None
    }
    case Event(Failure(f), jobOpt) => log.error(f, "Error occurred while working"); throw f
  }

  whenUnhandled {
    case Event(unhandledMessage, data) =>
      log.error(s"Error unhandled message: $unhandledMessage" + stay())
      stay() using data
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    master ! GiveMeWorkToDo()
    startWith(WaitingForJob, None)
  }

}