package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.common.model.{ Next, NextVal }

object SplitterActor {
  def props[A](target: ActorRef) = Props(new SplitterActor(target))
}

@SerialVersionUID(1L)
case class SplitterActor[A](destination: ActorRef)
    extends Actor with StandardAtLeastOnceDelivery[A] {

  override def receive: Receive = {
    case n: NextVal[Seq[A] @unchecked] =>
      n.data.zipWithIndex.foreach { case (data, index) => deliver(destination, Next(index, data)) }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

object SplitterPersistentActor {
  def props[A](id: String, target: ActorRef) = Props(new SplitterPersistentActor(id, target))
}

case class SplitterPersistentActor[A](id: String, destination: ActorRef)
    extends PersistentActor
    with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case n: NextVal[Seq[A] @unchecked] =>
      n.data.zipWithIndex.foreach { case (data, index) => deliver(destination, Next(index, data)) }
  }

  override def persistenceId: String = "id"

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[Seq[A] @unchecked] =>
      persistAsync(n) { e =>
        e.data.zipWithIndex.foreach { case (data, index) => deliver(destination, Next(index, data)) }
      }

  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}
