package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow_common.model.NextVal

object ConnectorActor {
  def props(dest: ActorRef) = Props(new ConnectorActor(dest))
}

case class ConnectorActor(destination: ActorRef)
    extends Actor with StandardAtLeastOnceDelivery[Any] {

  def receive = {
    case n: NextVal[_] => deliver(destination, n)
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

object ConnectorPersistentActor {
  def props(id: String, dest: ActorRef) = Props(new ConnectorPersistentActor(id, dest))
}

case class ConnectorPersistentActor(id: String, destination: ActorRef)
    extends PersistentActor
    with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case n: NextVal[_] => deliver(destination, n)
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[_] =>
      persistAsync(n) { e => deliver(destination, e) }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}
