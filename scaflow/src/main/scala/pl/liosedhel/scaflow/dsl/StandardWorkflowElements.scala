package pl.liosedhel.scaflow.dsl

import akka.actor.{ Address, SupervisorStrategy, ActorRef, ActorSystem }
import pl.liosedhel.scaflow.blocks.GroupActor.GroupStrategy
import pl.liosedhel.scaflow.blocks._
import pl.liosedhel.scaflow.blocks.map.MapMasterActor

class ConnectorElement[A]() extends WorkflowCommon[A] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(ConnectorActor.props(target))
  }
}

class SynchronizeElement[A](synchronizationStrategy: SynchronizationStrategy[A, Seq[A]]) extends WorkflowCommon[Seq[A]] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SynchronizeActor.props(synchronizationStrategy, target))
  }

  override def toString = s"SynchronizeElement()"
}

class SynchronizeElement2[A, B](synchronizationStrategy: SynchronizationStrategy[Any, (A, B)]) extends WorkflowCommon[(A, B)] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SynchronizeActor.props(synchronizationStrategy, target))
  }

  override def toString = s"SynchronizeElement()"
}

class MapPullElement[A, B](override var workflow: List[FlowElement], operation: A => B, customSupervisorStrategy: Option[SupervisorStrategy], workerNumber: Int, addresses: Seq[Address]) extends WorkflowCommon[B] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(MapMasterActor.props[A, B](operation, target, customSupervisorStrategy, workerNumber, addresses))
  }

  override def toString = s"MapPullElement()"
}

class SinkElement[A](override var workflow: List[FlowElement], operation: A => Unit) extends FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SinkActor.props(operation))
  }
}

class ExternalSinkElement[A](override var workflow: List[FlowElement], externalTarget: ActorRef) extends FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(ExternalSinkActor.props(externalTarget))
  }

  override def toString = s"SinkConnector()"
}

class PipeToRunningFlowElement[A](override var workflow: List[FlowElement], externalTarget: ActorRef) extends FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    externalTarget
  }

  override def toString = s"SinkConnector()"
}

class SourceElement[A](producer: => Seq[A], stopWorkflow: Boolean) extends WorkflowCommon[A] with FlowElement {
  override var workflow: List[FlowElement] = List(this)
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    SourceActor.run(actorSystem.actorOf(SourceActor.props(target, producer, stopWorkflow)))
  }

  override def toString = s"SourceElement()"
}

class GroupElement[A](override var workflow: List[FlowElement], groupStrategy: GroupStrategy[A]) extends WorkflowCommon[Seq[A]] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(GroupActor.props(target, groupStrategy))
  }

  override def toString = s"GroupElement()"
}

class GroupElementDefault[A](override var workflow: List[FlowElement], groupSize: Int) extends WorkflowCommon[Seq[A]] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(GroupActor.props(target, groupSize))
  }
}

class FilterElement[A](override var workflow: List[FlowElement], p: A => Boolean) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(FilterActor.props(target, p))
  }
}

class BroadcastElement[A](override var workflow: List[FlowElement], elements: Seq[FlowElement]) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(BroadcastActor.props[A](elements.map(_.run(actorSystem))))
  }
}

class PipeToElement[A](override var workflow: List[FlowElement], chainWorkflow: List[FlowElement], element: WorkflowCommon[A] with FlowElement) extends WorkflowCommon[A] with FlowElement {
  workflow = (workflow :+ this) ++ chainWorkflow
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    target
  }

  override def toString = s"PipeToElement()"
}

class SplitterElement[A](override var workflow: List[FlowElement]) extends WorkflowCommon[A] with FlowElement {
  workflow = workflow :+ this
  override protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef = {
    actorSystem.actorOf(SplitterActor.props[A](target))
  }

  override def toString = s"SplitterElement()"
}

trait WorkflowCommon[A] {
  var workflow: List[FlowElement]
}

trait FlowElement extends RunElement {

  var workflow: List[FlowElement]

  protected[dsl] def connect(actorSystem: ActorSystem, target: ActorRef): ActorRef
}

trait RunElement {
  var workflow: List[FlowElement]

  def reverse = {
    workflow.reverse
  }

  def run(implicit actorSystem: ActorSystem): ActorRef = {
    workflow.reverse match {
      case sink :: tail => tail.foldLeft(sink.connect(actorSystem, null)) {
        case (target, flowElement) => flowElement.connect(actorSystem, target)
      }
      case Nil => throw new RuntimeException("There is no runnable element in workflow")
    }
  }
}