package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow_common.model.NextVal

object FilterActor {
  def props[A](destination: ActorRef, filter: A => Boolean) = Props(FilterActor(destination, filter))
}

case class FilterActor[A](destination: ActorRef,
  filter: A => Boolean)
    extends Actor with StandardAtLeastOnceDelivery[A] {

  def receive = {
    case n: NextVal[A] =>
      if (filter(n.data)) deliver(destination, n)
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

object FilterPersistentActor {
  def props[A](id: String, destination: ActorRef, filter: A => Boolean) = Props(FilterPersistentActor(id, destination, filter))
}

case class FilterPersistentActor[A](id: String,
  destination: ActorRef,
  filter: A => Boolean)
    extends PersistentActor
    with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case n: NextVal[A] =>
      if (filter(n.data)) deliver(destination, n)
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[A] =>
      persistAsync(n) { e =>
        if (filter(e.data)) deliver(destination, n)
      }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

