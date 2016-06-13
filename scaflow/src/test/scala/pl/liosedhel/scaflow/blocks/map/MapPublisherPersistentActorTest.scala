package pl.liosedhel.scaflow.blocks.map

import akka.actor._
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentUniqueMessage
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Master.{ GiveMeWorkToDo, WorkDone, WorkerCreated }
import pl.liosedhel.scaflow.common.model.CommonMasterWorkerModel.Worker.WorkIsReady
import pl.liosedhel.scaflow.common.model.Next

class MapPublisherPersistentActorTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MapPublisherActorTest"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "MapPublisher actor" must {
    "acquire new messages" in {
      val destination = TestProbe()

      val mapActorRef = system.actorOf(Props(classOf[MapMasterPersistentActor[Int, Int]], "publisher", (a: Int) => a + 1, destination.ref, None, 1, Seq.empty))

      mapActorRef ! Next(1, 5)

      destination.expectMsgPF() { case PersistentUniqueMessage(_, Next(1, 6)) => true }
    }

    "worker must get message work ready" in {
      val destination = TestProbe()

      val worker = TestProbe()

      class WithoutWorker extends MapMasterPersistentActor("publisher2", (a: Int) => a + 1, destination.ref, None, 0, Seq.empty) {
        override protected def generateWorkers(context: ActorContext, workersNumber: Int, addresses: Seq[Address]): Unit = {
          self ! WorkerCreated(worker.ref)
        }
      }

      val mapActorRef = system.actorOf(Props(new WithoutWorker))

      mapActorRef ! Next(1, 5)

      worker.expectMsg(WorkIsReady())

      worker.send(mapActorRef, GiveMeWorkToDo())

      worker.expectMsg(Next(1, 5))

      worker.send(mapActorRef, WorkDone(Next(1, 5), 5))

      worker.expectNoMsg()

    }
  }

}