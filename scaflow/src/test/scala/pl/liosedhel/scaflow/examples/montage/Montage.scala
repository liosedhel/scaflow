package pl.liosedhel.scaflow.examples.montage

import akka.actor.{ ActorRef, ActorSystem, Props }
import pl.liosedhel.scaflow.blocks.GroupActor.GroupByKeyId
import pl.liosedhel.scaflow.blocks.map.MapMasterActor
import pl.liosedhel.scaflow.blocks.{ DynamicConnectorActor, GroupActor, SourceActor }
import pl.liosedhel.scaflow.dsl.{ FlowElement, PersistentWorkflow, StandardWorkflow }

object Montage {

  def run(actorSystem: ActorSystem, length: Int) = {

    def operation(seq: Seq[Int]): Int = {
      seq.sum
    }
    def montageFlow(): Props = {
      lazy val groupActor = GroupActor.props(sumActor, new GroupByKeyId[Any](2))
      lazy val sumActor = actorSystem.actorOf(MapMasterActor.props(operation, connector, None, 1, Seq.empty))
      lazy val connector = actorSystem.actorOf(DynamicConnectorActor.props(montageFlow))
      groupActor
    }

    lazy val sourceActor = actorSystem.actorOf(SourceActor.props(montage, 1 to length, stopWorkflow = false))
    lazy val montage = actorSystem.actorOf(montageFlow())
    SourceActor.run(sourceActor)

  }

  val lnOf2 = scala.math.log(2) // natural log of 2
  def log2(x: Double): Double = scala.math.log(x) / lnOf2

  def dslMontage(length: Int, resultActor: ActorRef)(implicit actorSystem: ActorSystem) = {

    def operation(seq: Seq[Int]): Int = {
      seq.sum
    }

    def montageFlow(step: Int, workflow: StandardWorkflow[Int] with FlowElement): StandardWorkflow[Int] with FlowElement = {
      if (step == 0)
        workflow
      else
        montageFlow(step - 1, workflow.pipeTo(StandardWorkflow.emptyWorkflow[Int]().group(new GroupByKeyId[Int](2)).map(operation)))
    }

    val levels = log2(length.toDouble).toInt
    val montage = montageFlow(levels, StandardWorkflow.connector())
    StandardWorkflow.source(1 to length).pipeTo(montage).sink(resultActor).run

  }

  def montageManualDsl(resultActor: ActorRef)(implicit actorSystem: ActorSystem) = {
    def group = StandardWorkflow.connector[Int]().group(2)
    def sum = StandardWorkflow.connector[Seq[Int]]().map(_.sum)

    val group2_1 = group.pipeTo(sum).sink(resultActor).run

    val group1_1 = group.pipeTo(sum).pipeTo(group2_1).run
    val group1_2 = group.pipeTo(sum).pipeTo(group2_1).run

    StandardWorkflow.source(Seq(1), stopWorkflow = false).pipeTo(group1_1).run
    StandardWorkflow.source(Seq(2), stopWorkflow = false).pipeTo(group1_1).run
    StandardWorkflow.source(Seq(3), stopWorkflow = false).pipeTo(group1_2).run
    StandardWorkflow.source(Seq(4), stopWorkflow = false).pipeTo(group1_2).run
  }

  def montageManualPersistentDsl(resultActor: ActorRef)(implicit actorSystem: ActorSystem) = {
    def group(id: String) = PersistentWorkflow.connector[Int](s"$id-connector").group(s"$id-group", 2)
    def sum(id: String) = PersistentWorkflow.connector[Seq[Int]](s"$id-connector").map(s"$id-sum", _.sum)

    val group2_1 = group("level2_1group").pipeTo(sum("level2_1sum")).sink(resultActor).run

    val group1_1 = group("level1_1group").pipeTo(sum("level1_1sum")).pipeTo(group2_1).run
    val group1_2 = group("level1_2group").pipeTo(sum("level1_2sum")).pipeTo(group2_1).run

    PersistentWorkflow.source("1", Seq(1), stopWorkflow = false).pipeTo(group1_1).run
    PersistentWorkflow.source("2", Seq(2), stopWorkflow = false).pipeTo(group1_1).run
    PersistentWorkflow.source("3", Seq(3), stopWorkflow = false).pipeTo(group1_2).run
    PersistentWorkflow.source("4", Seq(4), stopWorkflow = false).pipeTo(group1_2).run
  }
}
