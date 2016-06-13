package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.common.model.NextVal

object BroadcastActor {
  def props[A](targets: Seq[ActorRef]) = Props(new BroadcastActor[A](targets))
}

case class BroadcastActor[A](destinations: Seq[ActorRef])
    extends Actor with StandardAtLeastOnceDelivery[A] {

  override def receive: Receive = {
    case n: NextVal[A] => destinations.foreach(target => deliver(target, n))
  }
}

object BroadcastPersistentActor {
  def props[A](id: String, targets: Seq[ActorRef]) =
    Props(new BroadcastPersistentActor[A](id, targets))
}

case class BroadcastPersistentActor[A](id: String,
  destinations: Seq[ActorRef])
    extends PersistentActor with PersistentAtLeastOnceDelivery {

  override def persistenceId: String = id

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case n: NextVal[_] =>
      destinations.foreach(target => deliver(target, n))
  }

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[_] =>
      persistAsync(n) { _ =>
        destinations.foreach(target => deliver(target, n))
      }
  }

}
