package pl.liosedhel.scaflow.examples.montage

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class MontageTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "An montage" must {

    "construct and run montage using Workflow DSL" in {
      import scala.concurrent.duration._
      for (i <- 1 to 100) {
        val testProbe = TestProbe()
        Montage.dslMontage(32, testProbe.ref)

        testProbe.expectMsgPF(10.seconds) { case 528 => true }
      }

    }

    "construct and run montage using only Workflow DSL" in {
      val resultActor = TestProbe()
      Montage.montageManualDsl(resultActor.ref)

      resultActor.expectMsgPF() { case 10 => true }

    }

    "construct and run montage using only Persistent Workflow DSL" in {
      val resultActor = TestProbe()
      Montage.montageManualPersistentDsl(resultActor.ref)

      resultActor.expectMsgPF() { case 10 => true }

    }

  }
}
