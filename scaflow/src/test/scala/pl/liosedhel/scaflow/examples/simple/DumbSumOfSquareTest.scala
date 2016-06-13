package pl.liosedhel.scaflow.examples.simple

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.ack.StandardAckModel.UniqueMessage
import pl.liosedhel.scaflow.common.model.Next

class DumbSumOfSquareTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Dumb sum of square framework" must {
    "count sum of square" in {
      val resultActor = TestProbe()
      //WHEN
      DumbSumOfSquare.run(List(1, 2, 3), system, resultActor.ref)
      //THEN
      resultActor.expectMsgPF() { case UniqueMessage(_, Next(1, 14)) => true }
    }

    "count buggy sum of square properly" in {
      val resultActor = TestProbe()
      //WHEN
      DumbSumOfSquare.runBuggy(List(1, 2, 3), system, resultActor.ref)
      //THEN
      resultActor.expectMsgPF() { case UniqueMessage(_, Next(1, 14)) => true }
    }
  }
}

