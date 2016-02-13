package pl.liosedhel.scaflow.blocks.map

import pl.liosedhel.scaflow.blocks.map.PullModel.MasterState._

import scala.collection.mutable

object PullModel {
  trait Event

  object MasterState {
    case class NewWorkerEvent[Worker](worker: Worker) extends Event
    case class NewMessageEvent[Job](n: Job) extends Event
    case class WorkDoneEvent[Worker, Job](worker: Worker, n: Job, result: Any) extends Event
    case class WorkerTerminatedEvent[Worker](worker: Worker) extends Event
    case class TakeMessageEvent[Worker](worker: Worker) extends Event
    case class DropActualWorkerWorkEvent[Worker](worker: Worker) extends Event
  }

  trait MasterState[Worker, Work] {

    protected val state: State[Worker, Work] = new State[Worker, Work]()

    def resetState() = {
      state.resetState()
    }

    def updateState(event: Event): Unit = event match {
      case NewMessageEvent(n: Work @unchecked) => state.addWork(n)
      case TakeMessageEvent(w: Worker @unchecked) => state.createJobFor(w);
      case WorkDoneEvent(w: Worker @unchecked, job: Work @unchecked, result) => state.workDone(w, job);
      case WorkerTerminatedEvent(w: Worker @unchecked) => state.markWorkerAsDead(w)
      case NewWorkerEvent(w: Worker @unchecked) => state.addWorker(w)
      case DropActualWorkerWorkEvent(w: Worker @unchecked) => state.dropActualWorkerWork(w);
    }

    def dropActualWorkerJob(worker: Worker) = {
      state.dropActualWorkerWork(worker) //drop old work
    }

    def getExistingJobFor(worker: Worker): Option[Any] = {
      state.workerJobMap.getOrElse(worker, None)
    }

    def isJobPending: Boolean = {
      state.isJobPending
    }

    def getAllFreeWorkers: Seq[Worker] = {
      state.getAllFreeWorkers
    }

    def isAllWorkDone = {
      state.isWorkAllWorkDone
    }
  }

  class State[Worker, Work]() {

    var workToDo = mutable.Queue.empty[Work]
    var workerJobMap = mutable.Map.empty[Worker, Option[Work]]

    def resetState() = {
      workerJobMap.values.flatten.foreach(addWork)
      workerJobMap = mutable.Map.empty[Worker, Option[Work]]
    }

    def addWorker(worker: Worker): Unit = {
      if (!workerJobMap.isDefinedAt(worker)) {
        workerJobMap += worker -> None
      }
    }

    def deleteWorker(worker: Worker): Unit = {
      workerJobMap.getOrElse(worker, Option.empty).foreach { job =>
        workToDo += job
      }
      workerJobMap -= worker
    }

    def addWork(work: Work) = {
      workToDo += work
    }

    def createJobFor(worker: Worker): Option[Work] = {
      workerJobMap.get(worker).fold(Option.empty[Work]) {
        case work @ Some(_) => work
        case None =>
          if (workToDo.isEmpty) {
            None
          } else {
            val work = Some(workToDo.dequeue())
            workerJobMap.put(worker, work)
            work
          }
      }
    }

    def markWorkerAsDead(deadWorker: Worker) = {
      workerJobMap.remove(deadWorker).flatten.map(workToDo += _)
    }

    def getAllFreeWorkers: Seq[Worker] = {
      workerJobMap.filter { case (_, work) => work.isEmpty }.keys.toSeq
    }

    def workDone(worker: Worker, work: Work) = {
      workerJobMap.put(worker, None)
    }

    def dropActualWorkerWork(worker: Worker) = {
      workerJobMap.put(worker, None)
    }

    def isJobPending = workToDo.nonEmpty

    def isWorkAllWorkDone = !isJobPending && workerJobMap.values.forall(_.isEmpty)
  }
}
