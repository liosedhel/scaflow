package pl.liosedhel.scaflow.dsl

import java.util.UUID

import akka.actor.{ Address, ActorRef, SupervisorStrategy }
import pl.liosedhel.scaflow.blocks.GroupActor.GroupStrategy

object PersistentWorkflow {
  def source[A](id: String, producer: => Seq[A], stopWorkflow: Boolean = true): PersistentWorkflow[A] = {
    new SourceElementPersistent(id, producer, stopWorkflow) with PersistentWorkflow[A]
  }

  def connector[A](id: String) = {
    new ConnectorElementPersistent[A](id) with PersistentWorkflow[A]
  }

  def emptyWorkflow[A]() = {
    new PersistentWorkflow[A] {
      override var workflow: List[FlowElement] = List.empty[FlowElement]
    }
  }
}

sealed trait PersistentSink[E] extends FlowElement {
  self: FlowElement =>
}

trait PersistentWorkflow[A] extends WorkflowCommon[A] {

  def filter(id: String, predicate: A => Boolean): PersistentWorkflow[A] with FlowElement = {
    new FilterElementPersistent(workflow, id, predicate) with PersistentWorkflow[A]
  }

  def sink(id: String, operation: A => Unit): PersistentSink[A] = {
    new SinkElementPersistent(id, workflow, operation) with PersistentWorkflow[A] with PersistentSink[A]
  }

  def sink(actorRef: ActorRef, id: String = UUID.randomUUID().toString): PersistentSink[A] = {
    new ExternalSinkElementPersistent[A](id, workflow, actorRef) with PersistentSink[A]
  }

  def pipeTo(runningFlow: ActorRef): FlowElement = {
    new PipeToRunningFlowElement[A](workflow, runningFlow)
  }

  def broadcast(id: String, elements: FlowElement*) = {
    new BroadcastElementPersistent[A](id, workflow, elements) with PersistentWorkflow[A]
  }

  def broadcast(id: String, runningFlows: ActorRef*) = {
    new PipeBroadcastElementPersistent[A](id, workflow, runningFlows) with PersistentWorkflow[A]
  }

  def pipeTo[B](element: PersistentWorkflow[B] with FlowElement) = {
    new PipeToElement(workflow, element.workflow, element) with PersistentWorkflow[B]
  }

  def group(id: String, groupStrategy: GroupStrategy[A]) = {
    new GroupElementPersistent[A](id, workflow, groupStrategy) with PersistentWorkflow[Seq[A]]
  }

  def group(id: String, groupSize: Int): PersistentWorkflow[Seq[A]] = {
    new GroupElementPersistentDefault[A](id, workflow, groupSize) with PersistentWorkflow[Seq[A]]
  }

  def mapWithOrder[B](id: String, operation: A => B, customSupervisorStrategy: Option[SupervisorStrategy] = None) = {
    map[B](id, operation, customSupervisorStrategy, 1)
  }

  def map[B](id: String, operation: A => B, customSupervisorStrategy: Option[SupervisorStrategy] = None, workersNumber: Int = 8, addresses: Seq[Address] = Seq.empty) = {
    new MapPersistentPullElement[A, B](id, workflow, operation, customSupervisorStrategy, workersNumber, addresses) with PersistentWorkflow[B]
  }

  def split[B](id: String): PersistentWorkflow[B] = {
    new SplitterElementPersistent[B](id, workflow) with PersistentWorkflow[B]
  }
}

