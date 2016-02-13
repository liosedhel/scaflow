package pl.liosedhel.scaflow.dsl

import akka.actor.{ Address, SupervisorStrategy, ActorRef, ActorSystem }
import pl.liosedhel.scaflow.blocks.GroupActor.GroupStrategy
import pl.liosedhel.scaflow.blocks._
import pl.liosedhel.scaflow.blocks.map.MapMasterPersistentActor

class SourceElementPersistent[A](id: String, producer: => Seq[A], stopWorkflow: Boolean) extends WorkflowCommon[A] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    SourcePersistentActor.run(actorSystem.actorOf(SourcePersistentActor.props(id, target, producer, stopWorkflow)))
  }
}

class MapPersistentPullElement[A, B](id: String, override var workflow: List[FlowElement], operation: A => B, customSupervisorStrategy: Option[SupervisorStrategy], workersNumber: Int, addresses: Seq[Address]) extends WorkflowCommon[B] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(MapMasterPersistentActor.props[A, B](id, operation, target, customSupervisorStrategy, workersNumber, addresses))
  }
}

class GroupElementPersistent[A](id: String, override var workflow: List[FlowElement], groupStrategy: GroupStrategy[A]) extends WorkflowCommon[Seq[A]] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(GroupPersistentActor.props(id, target, groupStrategy))
  }
}

class GroupElementPersistentDefault[A](id: String, override var workflow: List[FlowElement], groupSize: Int) extends WorkflowCommon[Seq[A]] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(GroupPersistentActor.props(id, target, groupSize))
  }
}

class FilterElementPersistent[A](override var workflow: List[FlowElement], id: String, p: A => Boolean) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(FilterPersistentActor.props(id, target, p))
  }
}

class SinkElementPersistent[A](id: String, override var workflow: List[FlowElement], operation: A => Unit) extends FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SinkPersistentActor.props(id, operation))
  }
}

class ExternalSinkElementPersistent[A](id: String, override var workflow: List[FlowElement], externalTarget: ActorRef) extends FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(ExternalSinkPersistentActor.props(id, externalTarget))
  }
}

class BroadcastElementPersistent[A](id: String, override var workflow: List[FlowElement], elements: Seq[FlowElement]) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(BroadcastPersistentActor.props[A](id, elements.map(_.run(actorSystem))))
  }
}

class PipeBroadcastElementPersistent[A](id: String, override var workflow: List[FlowElement], destinations: Seq[ActorRef]) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(BroadcastPersistentActor.props[A](id, destinations))
  }
}

class ConnectorElementPersistent[A](id: String) extends WorkflowCommon[A] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(ConnectorPersistentActor.props(id, target))
  }
}

class SplitterElementPersistent[A](id: String, override var workflow: List[FlowElement]) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SplitterPersistentActor.props[A](id, target))
  }
}