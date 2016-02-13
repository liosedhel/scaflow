package pl.liosedhel.scaflow.blocks.map

import akka.actor._
import akka.remote.routing.RemoteRouterConfig
import akka.routing.RoundRobinPool
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.map.PullModel.MasterState
import pl.liosedhel.scaflow.blocks.map.PullModel.MasterState._
import pl.liosedhel.scaflow_common.model.CommonMasterWorkerModel.Master.{ WorkDone, GiveMeWorkToDo, WorkerCreated }
import pl.liosedhel.scaflow_common.model.CommonMasterWorkerModel.Worker.{ WorkIsReady, NoMoreWorkToDo }
import pl.liosedhel.scaflow_common.model.{ Next, NextVal }
import pl.liosedhel.scaflow.strategy.FailuresStrategies.{ DropMessage, OverridenSupervisorStrategy }
import pl.liosedhel.scaflow_common.workers.MapWorkerActor

object MapMasterActor {

  def props[A, B](operation: A => B,
    destination: ActorRef,
    customSupervisorStrategy: Option[SupervisorStrategy],
    workersNumber: Int,
    addresses: Seq[Address]) =
    Props(new MapMasterActor(operation, destination, customSupervisorStrategy, workersNumber, addresses))
}

case class MapMasterActor[A, B](operation: A => B,
  destination: ActorRef,
  customSupervisorStrategy: Option[SupervisorStrategy],
  workersNumber: Int,
  addresses: Seq[Address])
    extends Actor with MasterState[ActorRef, Any]
    with ActorLogging with StandardAtLeastOnceDelivery[B]
    with DropMessage {

  override def supervisorStrategy: SupervisorStrategy =
    customSupervisorStrategy.map(OverridenSupervisorStrategy(this)).getOrElse(super.supervisorStrategy)

  override def dropMessage(): Unit = {
    dropActualWorkerJob(sender())
    checkIfAllDataProcessed()
  }

  protected def generateWorkers(context: ActorContext, workersNumber: Int, addresses: Seq[Address]): Unit = {
    if (addresses.isEmpty) {
      for (i <- 1 to workersNumber) context.actorOf(MapWorkerActor.props(self, operation))
    } else {
      context.actorOf(
        RemoteRouterConfig(RoundRobinPool(workersNumber, supervisorStrategy = supervisorStrategy), addresses).props(MapWorkerActor.props(self, operation)))
    }
  }

  generateWorkers(context, workersNumber, addresses)

  override def receive: Receive = {
    case WorkerCreated(worker) =>
      updateState(NewWorkerEvent(worker))
      context.watch(worker)
      notifyWorkers()

    case Terminated(w) =>
      updateState(WorkerTerminatedEvent(w))

    case n: NextVal[A] =>
      updateState(NewMessageEvent(n))
      notifyWorkers()

    case GiveMeWorkToDo() =>
      val worker = sender()
      updateState(TakeMessageEvent(worker))
      val message = getExistingJobFor(worker)
      message match {
        case None => worker ! NoMoreWorkToDo()
        case Some(work) => worker ! work
      }

    case WorkDone(n, result: B @unchecked) =>
      val resultToSend = Next(n.id, result)
      deliver(destination, resultToSend)
      updateState(WorkDoneEvent(sender(), n, resultToSend))
      notifyWorkers()
  }

  def notifyWorkers(): Unit = {
    if (isJobPending) {
      getAllFreeWorkers.foreach { _ ! WorkIsReady() }
    }
  }

  /**
   * In case of custom concurrent job transforming or if additional waiting is needed
   *
   * @return
   */
  override def allJobProcessed(): Boolean = isAllWorkDone

  override def destinations: Seq[ActorRef] = Seq(destination)
}

