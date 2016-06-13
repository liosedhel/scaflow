package pl.liosedhel.scaflow.ack

import akka.actor._
import akka.persistence.PersistentActor
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.ack.PersistentAckModel.{ PersistentAck, PersistentUniqueMessage, PersistentAtLeastOnceDelivery }
import pl.liosedhel.scaflow.common.model.{ Next, NextVal }

import scala.concurrent.duration.{ FiniteDuration, _ }

class PersistentAtLeastOnceDeliveryTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  case class AtLeastOnceDeliveryActor(destination: ActorRef)
      extends PersistentActor
      with PersistentAtLeastOnceDelivery {

    override def persistenceId: String = "idAtLeastOnceDelivery"

    override def redeliverInterval: FiniteDuration = 150.millis

    override def receiveRecover: Receive = receiveRecoverWithAck {
      case n: NextVal[Int] => deliver(destination, n)
    }

    override def receiveCommand: Receive = receiveCommandWithAck {
      case n: NextVal[Int] => persistAsync(n) { ne =>
        deliver(destination, Next(ne.id, n.data))
      }
      case Boom() =>
    }

    override def destinations: Seq[ActorRef] = Seq(destination)

  }

  case class Boom()

  "AtLestOnceDelivery" must {
    "repeat not confirmed message" in {
      val nextActor = TestProbe()
      val testActor = system.actorOf(Props(new AtLeastOnceDeliveryActor(nextActor.ref)))
      val prevActor = TestProbe()

      prevActor.send(testActor, PersistentUniqueMessage(1, Next(1, 1)))
      prevActor.expectMsgPF() { case PersistentAck(1L) => true }
      nextActor.expectMsgPF() { case PersistentUniqueMessage(_, Next(1, 1)) => true }
      nextActor.expectMsgPF() { case PersistentUniqueMessage(_, Next(1, 1)) => true }
      testActor ! PersistentUniqueMessage(2, Boom())
      nextActor.expectMsgPF() {
        case PersistentUniqueMessage(deliverId, Next(1, 1)) => {
          nextActor.send(testActor, PersistentAck(deliverId))
        }
      }

      nextActor.expectNoMsg(300.millis)
    }
  }
}
