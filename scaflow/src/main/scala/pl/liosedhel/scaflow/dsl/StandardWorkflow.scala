package pl.liosedhel.scaflow.dsl

import akka.actor.{ Address, ActorRef, SupervisorStrategy }
import pl.liosedhel.scaflow.blocks.GroupActor.GroupStrategy
import pl.liosedhel.scaflow.blocks.{ DefaultSynchronizationStrategy, GroupActor, TypeBasedSynchronization2 }

object StandardWorkflow {

  def source[A](producer: => Seq[A], stopWorkflow: Boolean = true) = {
    new SourceElement(producer, stopWorkflow) with StandardWorkflow[A]
  }

  def connector[A]() = {
    new ConnectorElement[A]() with StandardWorkflow[A]
  }

  def emptyWorkflow[A]() = {
    new StandardWorkflow[A] {
      override var workflow: List[FlowElement] = List.empty[FlowElement]
    }
  }

  def synchronization[A](sourceNumber: Int) = {
    new SynchronizeElement[A](new DefaultSynchronizationStrategy[A](sourceNumber)) with StandardWorkflow[Seq[A]]
  }

  def synchronization2[A, B](convert: (Any, Any) => (A, B)): StandardWorkflow[(A, B)] = {
    new SynchronizeElement2[A, B](new TypeBasedSynchronization2(convert)) with StandardWorkflow[(A, B)]
  }

}

trait StandardWorkflow[A] extends WorkflowCommon[A] {

  def filter(p: A => Boolean) = {
    new FilterElement(workflow, p) with StandardWorkflow[A]
  }

  def group[B](groupStrategy: GroupStrategy[A]) = {
    new GroupElement(workflow, groupStrategy) with StandardWorkflow[Seq[A]]
  }

  def group(groupSize: Int) = {
    new GroupElementDefault[A](workflow, groupSize) with StandardWorkflow[Seq[A]]
  }

  def sink(operation: A => Unit) = {
    new SinkElement(workflow, operation) with StandardWorkflow[A]
  }

  def sink(actorRef: ActorRef): FlowElement = {
    new ExternalSinkElement[A](workflow, actorRef)
  }

  def pipeTo(actorRef: ActorRef): FlowElement = {
    new PipeToRunningFlowElement[A](workflow, actorRef)
  }

  def broadcast(elements: FlowElement*) = {
    new BroadcastElement[A](workflow, elements) with StandardWorkflow[A]
  }

  def pipeTo[B](element: WorkflowCommon[B] with FlowElement) = {
    new PipeToElement(workflow, element.workflow, element) with StandardWorkflow[B]
  }

  def map[B](operation: A => B,
    customSupervisorStrategy: Option[SupervisorStrategy] = None,
    workersNumber: Int = 8,
    addresses: Seq[Address] = Seq.empty) = {
    new MapPullElement[A, B](
      workflow,
      operation,
      customSupervisorStrategy,
      workersNumber,
      addresses) with StandardWorkflow[B]
  }

  def split[B](): StandardWorkflow[B] = {
    new SplitterElement[B](workflow) with StandardWorkflow[B]
  }
}