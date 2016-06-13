package pl.liosedhel.scaflow.common.model

import akka.actor.ActorRef

object CommonMasterWorkerModel {

  object Master {
    case class GiveMeWorkToDo()
    case class WorkDone[A, B](n: NextVal[A], result: B)
    case class WorkerCreated(worker: ActorRef)
  }

  object Worker {
    case class WorkIsReady()
    case class WorkCompleted[A](result: A)
    case class NoMoreWorkToDo()
  }

}
