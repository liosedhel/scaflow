package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorContext, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow_common.model.NextVal

object SinkActor {
  def props[A](operation: A => Unit) = Props(new SinkActor[A](operation))
}
class SinkActor[A](operation: A => Unit)
    extends Actor with StandardAtLeastOnceDelivery[A] {

  override def receive = {
    case data: NextVal[A] => operation(data.data)
  }

  override def destinations: Seq[ActorRef] = Seq.empty[ActorRef]
}

object SinkPersistentActor {
  def props[A](id: String, operation: A => Unit) =
    Props(new SinkPersistentActor[A](id, operation))
}
class SinkPersistentActor[A](id: String, operation: A => Unit)
    extends PersistentActor with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case _ =>
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = receiveCommandWithAck {
    case data: NextVal[A] => operation(data.data)
  }

  override def destinations: Seq[ActorRef] = Seq.empty[ActorRef]

}

object ExternalSinkActor {
  def props[A](externalTarget: ActorRef) =
    Props(new ExternalSinkActor[A](externalTarget))
}
class ExternalSinkActor[A](externalTarget: ActorRef) extends Actor
    with StandardAtLeastOnceDelivery[A] {

  override def receive = {
    case n: NextVal[A] => externalTarget ! n.data
  }

  override def destinations: Seq[ActorRef] = Seq(externalTarget)

  override def allDataProcessed(context: ActorContext, self: ActorRef): Unit = {
    destinations.foreach(_ ! AllDataProcessed())
    context.stop(self)
  }
}

object ExternalSinkPersistentActor {
  def props[A](id: String, externalTarget: ActorRef) =
    Props(new ExternalSinkPersistentActor[A](id, externalTarget))
}

class ExternalSinkPersistentActor[A](id: String, externalTarget: ActorRef)
    extends PersistentActor with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case _ =>
  }

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[A] => externalTarget ! n.data
  }

  override def destinations: Seq[ActorRef] = Seq(externalTarget)

  override def allDataProcessed(context: ActorContext, self: ActorRef): Unit = {
    destinations.foreach(_ ! AllDataProcessed())
    context.stop(self)
  }

  override def persistenceId: String = id
}