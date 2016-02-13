package pl.liosedhel.scaflow.examples.simple

import akka.actor.{ ActorRef, ActorSystem }
import pl.liosedhel.scaflow.blocks.map.MapMasterActor
import pl.liosedhel.scaflow.blocks.{ GroupActor, SourceActor }

import scala.util.Random

object DumbSumOfSquare {

  def run(nSeq: List[Int], actorSystem: ActorSystem, sinkActor: ActorRef) {
    def square(n: Int) = n * n
    def sum(seq: Seq[Int]) = seq.sum
    lazy val sumOfSquareFlow = actorSystem.actorOf(MapMasterActor.props(square, groupActor, None, 1, Seq.empty))
    lazy val groupActor = actorSystem.actorOf(GroupActor.props(sumActor, 3))
    lazy val sumActor = actorSystem.actorOf(MapMasterActor.props(sum, sinkActor, None, 1, Seq.empty))

    lazy val source = actorSystem.actorOf(SourceActor.props(sumOfSquareFlow, nSeq, stopWorkflow = true))
    SourceActor.run(source)
  }

  def runBuggy(nSeq: List[Int], actorSystem: ActorSystem, sinkActor: ActorRef) {
    def square(n: Int) = if (Random.nextDouble() < 0.5) n * n else throw new RuntimeException
    def sum(seq: Seq[Int]) = if (Random.nextDouble() < 0.5) seq.sum else throw new RuntimeException
    lazy val sumOfSquareFlow = actorSystem.actorOf(MapMasterActor.props(square, groupActor, None, 1, Seq.empty))
    lazy val groupActor = actorSystem.actorOf(GroupActor.props(sumActor, 3))
    lazy val sumActor = actorSystem.actorOf(MapMasterActor.props(sum, sinkActor, None, 1, Seq.empty))

    lazy val source = actorSystem.actorOf(SourceActor.props(sumOfSquareFlow, nSeq, stopWorkflow = true))
    SourceActor.run(source)
  }

}
