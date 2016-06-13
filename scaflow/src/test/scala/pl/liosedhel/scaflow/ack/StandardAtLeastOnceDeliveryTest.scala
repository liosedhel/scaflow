package pl.liosedhel.scaflow.ack

import java.util.UUID

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.ack.StandardAckModel.{ Ack, UniqueMessage, StandardAtLeastOnceDelivery }
import pl.liosedhel.scaflow.common.model.{ Next, NextVal }

import scala.concurrent.duration.{ FiniteDuration, _ }

class StandardAtLeastOnceDeliveryTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  case class StandardAtLeastOnceDeliveryActor(destination: ActorRef)
      extends StandardAtLeastOnceDelivery[Int] with Actor {

    override protected def redeliverInterval: FiniteDuration = 300.millis

    override def receive: Receive = {
      case n: NextVal[Int] => deliver(destination, Next(n.id, n.data))
    }

    override def destinations: Seq[ActorRef] = Seq(destination)
  }
  "AtLestOnceDelivery" must {
    "repeat not confirmed message" in {
      val nextActor = TestProbe()
      val testActor = system.actorOf(Props(new StandardAtLeastOnceDeliveryActor(nextActor.ref)))
      val prevActor = TestProbe()

      prevActor.send(testActor, UniqueMessage(UUID.randomUUID().toString, Next(1, 1)))
      prevActor.expectMsgPF() { case Ack(_) => true }
      nextActor.expectMsgPF() { case UniqueMessage(uuid, Next(1, 1)) => true }
      nextActor.expectMsgPF() { case UniqueMessage(uuid, Next(1, 1)) => true }
      nextActor.expectMsgPF() {
        case UniqueMessage(uuid, Next(1, 1)) => {
          nextActor.send(testActor, Ack(uuid))
        }
      }

      nextActor.expectNoMsg(300.millis)
    }
  }
}
