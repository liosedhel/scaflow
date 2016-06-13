package pl.liosedhel.scaflow.blocks.map

import akka.actor._
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import akka.remote.routing.RemoteRouterConfig
import akka.routing.RoundRobinPool
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.map.PullModel.{ Event, MasterState }
import pl.liosedhel.scaflow.blocks.map.PullModel.MasterState._
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Master.{ WorkDone, GiveMeWorkToDo, WorkerCreated }
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Worker.{ WorkIsReady, NoMoreWorkToDo }
import pl.liosedhel.scaflow.common.model.{ Next, NextVal }
import pl.liosedhel.scaflow.common.workers.MapWorkerActor
import pl.liosedhel.scaflow.strategy.FailuresStrategies.{ DropMessage, OverridenSupervisorStrategy }

object MapMasterPersistentActor {

  def props[A, B](id: String,
    operation: A => B,
    destination: ActorRef,
    customSupervisorStrategy: Option[SupervisorStrategy],
    workersNumber: Int,
    addresses: Seq[Address]) =
    Props(new MapMasterPersistentActor(id, operation, destination, customSupervisorStrategy, workersNumber, addresses))
}

case class MapMasterPersistentActor[A, B](id: String,
  operation: A => B,
  destination: ActorRef,
  customSupervisorStrategy: Option[SupervisorStrategy],
  workersNumber: Int,
  addresses: Seq[Address])
    extends PersistentActor
    with MasterState[ActorRef, Any] with ActorLogging
    with PersistentAtLeastOnceDelivery
    with DropMessage {

  override def supervisorStrategy: SupervisorStrategy =
    customSupervisorStrategy.map(OverridenSupervisorStrategy(this)).getOrElse(super.supervisorStrategy)

  override def dropMessage(): Unit = {
    persistAsync(DropActualWorkerWorkEvent(sender())) { e =>
      dropActualWorkerJob(e.worker)
      tryToStopActor()
      notifyWorkers()
    }
  }

  protected def generateWorkers(context: ActorContext, workersNumber: Int, addresses: Seq[Address]): Unit = {
    if (addresses.isEmpty) {
      for (i <- 1 to workersNumber) context.actorOf(MapWorkerActor.props(self, operation))
    } else {
      context.actorOf(
        RemoteRouterConfig(RoundRobinPool(workersNumber, supervisorStrategy = supervisorStrategy), addresses).props(MapWorkerActor.props(self, operation)))
    }
  }

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case w @ WorkDoneEvent(_, _, result) =>
      updateState(w); deliver(destination, result)
    case w @ DropActualWorkerWorkEvent(worker) =>
      updateState(w); tryToStopActor();
    case e: Event => updateState(e)
    case RecoveryCompleted => {
      resetState()
      generateWorkers(context, workersNumber, addresses)
    }
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = receiveCommandWithAck {
    case WorkerCreated(worker) =>
      persistAsync(NewWorkerEvent(worker)) { newWorkerEvent =>
        context.watch(worker)
        updateState(newWorkerEvent)
        notifyWorkers()
      }
    case Terminated(w) =>
      persistAsync(WorkerTerminatedEvent(w)) { workerTerminatedEvent =>
        updateState(workerTerminatedEvent)
      }

    case n: NextVal[A] =>
      persistAsync(NewMessageEvent(n)) { newMessageEvent =>
        updateState(newMessageEvent)
        notifyWorkers()
      }

    case GiveMeWorkToDo() =>
      persistAsync(TakeMessageEvent(sender())) { takeWorkEvent =>
        updateState(takeWorkEvent)
        val message = getExistingJobFor(takeWorkEvent.worker)
        message match {
          case None => takeWorkEvent.worker ! NoMoreWorkToDo()
          case Some(job) => takeWorkEvent.worker ! job
        }
      }
    case WorkDone(n, result) =>
      persistAsync(WorkDoneEvent(sender(), n, Next(n.id, result))) { jobDoneEvent =>
        updateState(jobDoneEvent)
        deliver(destination, jobDoneEvent.result)
        notifyWorkers()
      }
  }

  def notifyWorkers(): Unit = {
    if (isJobPending) {
      getAllFreeWorkers.foreach { _ ! WorkIsReady() }
    }
  }

  override def allJobProcessed(): Boolean = isAllWorkDone

  override def destinations: Seq[ActorRef] = Seq(destination)
}

