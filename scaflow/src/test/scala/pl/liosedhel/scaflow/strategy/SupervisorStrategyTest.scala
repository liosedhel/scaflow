package pl.liosedhel.scaflow.strategy

import java.util.UUID

import akka.actor.SupervisorStrategy.{ Escalate, Stop }
import akka.actor.{ ActorSystem, OneForOneStrategy }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.PersistentWorkflowTests
import pl.liosedhel.scaflow.ack.PersistentAckModel.{ PersistentAck, PersistentUniqueMessage }
import pl.liosedhel.scaflow.ack.StandardAckModel.{ Ack, UniqueMessage }
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow.blocks.map.{ MapMasterActor, MapMasterPersistentActor }
import pl.liosedhel.scaflow.dsl.PersistentWorkflow
import pl.liosedhel.scaflow_common.model.Next

class SupervisorStrategyTest(_system: ActorSystem)
    extends TestKit(_system)
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with PersistentWorkflowTests {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  import scala.concurrent.duration._

  "HttpSupervision" must {
    /*"fail after few times" in {
      val sink = TestProbe()
      def action(o: Any): Option[Any] = throw new RuntimeException()
      val mapActor = system.actorOf(MapMasterActor.props(action, sink.ref, Option(AdvancedSupervisorStrategyWrapper(FailuresStrategies.HTTPSupervisorStrategy)), 4))
      mapActor ! Next(1, 1)
      sink.expectNoMsg(3.seconds)
      //(1 to 2).foreach(i => mapActor ! Next(i, i))
      //sink.expectNoMsg(5.seconds)
    }*/

    "drop message when specific error occur" in {
      val sink = TestProbe()
      class DropMessageRuntimeException extends RuntimeException
      def action(a: Int): Int = {
        if (a % 2 == 0) {
          throw new DropMessageRuntimeException()
        } else {
          a
        }
      }

      val strategy = OneForOneStrategy(Int.MaxValue, 1.seconds) {
        case e: DropMessageRuntimeException => Stop;
        case _ => Escalate
      }

      val mapActor = system.actorOf(MapMasterActor.props(action, sink.ref, Option(strategy), 1, Seq.empty))
      mapActor ! UniqueMessage(UUID.randomUUID().toString, Next(1, 1))
      mapActor ! UniqueMessage(UUID.randomUUID().toString, Next(2, 2))
      mapActor ! UniqueMessage(UUID.randomUUID().toString, AllDataProcessed())
      sink.expectMsgPF() {
        case UniqueMessage(uuid, Next(1, 1)) =>
          {
            mapActor.tell(Ack(uuid), sink.ref)
          }; true
      }
      sink.expectMsgPF() {
        case UniqueMessage(uuid, AllDataProcessed()) => {
          mapActor.tell(Ack(uuid), sink.ref); true
        };
      }
      sink.expectNoMsg(3.seconds)
      //(1 to 2).foreach(i => mapActor ! Next(i, i))
      //sink.expectNoMsg(5.seconds)
    }

    "drop message in persistent map" in {
      val sink = TestProbe()
      class DropMessageRuntimeException extends RuntimeException
      def action(a: Int): Int = if (a % 2 == 1) {
        throw new DropMessageRuntimeException()
      } else {
        a
      }

      val strategy = OneForOneStrategy(Int.MaxValue, 1.seconds) {
        case e: DropMessageRuntimeException => Stop;
        case _ => Escalate
      }

      val mapActor = system.actorOf(MapMasterPersistentActor.props(UUID.randomUUID().toString, action, sink.ref, Option(strategy), 1, Seq.empty))
      mapActor ! PersistentUniqueMessage(1, Next(1, 1))
      mapActor ! PersistentUniqueMessage(2, Next(2, 2))
      mapActor ! PersistentUniqueMessage(3, AllDataProcessed())
      sink.expectMsgPF() {
        case PersistentUniqueMessage(uuid, Next(2, 2)) =>
          {
            mapActor.tell(PersistentAck(uuid), sink.ref)
          }; true
      }
      sink.expectMsgPF() {
        case PersistentUniqueMessage(uuid, AllDataProcessed()) => {
          mapActor.tell(PersistentAck(uuid), sink.ref); true
        };
      }
      sink.expectNoMsg(3.seconds)
      //(1 to 2).foreach(i => mapActor ! Next(i, i))
      //sink.expectNoMsg(5.seconds)
    }

    "drop message strategy in dsl" in {

      class DropMessageRuntimeException extends RuntimeException

      def canProcessOnlyOdd(a: Int): Int = if (a % 2 == 0) {
        throw new DropMessageRuntimeException()
      } else {
        a
      }

      val sink = TestProbe()
      val strategy = OneForOneStrategy(Int.MaxValue, 1.seconds) {
        case e: DropMessageRuntimeException => Stop;
        case _ => Escalate
      }

      PersistentWorkflow
        .source(uniqueId("source"), List(1, 2, 3))
        .map(uniqueId("onlyOdd"), canProcessOnlyOdd, customSupervisorStrategy = Some(strategy))
        .sink(sink.ref)
        .run

      sink.expectMsg(1)
      sink.expectMsg(3)
      sink.expectMsg(AllDataProcessed())
      sink.expectNoMsg(3.seconds)
    }

  }

}
