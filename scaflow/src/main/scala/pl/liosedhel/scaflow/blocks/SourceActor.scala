package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.AckModel._
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.SourceActor.{ AllDataProcessed, EndOfData, Start }
import pl.liosedhel.scaflow.common.model.Next

object SourceActor {
  case class Start()
  case class EndOfData()
  case class AllDataProcessed()

  def props[A](target: ActorRef, producer: => Seq[A], stopWorkflow: Boolean) = Props(new SourceActor(target, producer, stopWorkflow))
  def run(sourceActor: ActorRef) = {
    sourceActor ! Start()
    sourceActor
  }
}
class SourceActor[A](val destination: ActorRef, producer: => Seq[A], stopWorkflow: Boolean)
    extends Actor with StandardAtLeastOnceDelivery[A] {

  override def receive: Receive = {
    case Start() =>
      producer.zipWithIndex.foreach {
        case (data, index) =>
          deliver(destination, Next(index, data))
          if (stopWorkflow)
            deliver(self, AllDataProcessed())
      }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

object SourcePersistentActor {
  def props[A](id: String,
    target: ActorRef,
    producer: => Seq[A],
    stopWorkflow: Boolean) =
    Props(new SourcePersistentActor(id, target, producer, stopWorkflow))

  def run(sourceActor: ActorRef) = {
    sourceActor ! Start()
    sourceActor
  }
}

class SourcePersistentActor[A](id: String,
  val destination: ActorRef,
  producer: => Seq[A],
  stopWorkflow: Boolean)
    extends PersistentActor
    with PersistentAtLeastOnceDelivery {

  override def persistenceId: String = id

  override def receiveRecover: Receive = receiveCommandWithAck {
    case Start() =>
    case EndOfData() => sentState = true
  }

  var sentState = false

  override def receiveCommand: Receive = receiveCommandWithAck {
    case start @ Start() =>
      if (!sentState) {
        persistAsync(start) { _ =>
          producer.zipWithIndex.foreach { case (data, index) => deliver(destination, Next(index, data)) }
          deliver(self, EndOfData())
          if (stopWorkflow)
            deliver(self, AllDataProcessed())
        }
      }
    case endOfData @ EndOfData() => persistAsync(endOfData) { _ =>
      sentState = true
    }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)

}
