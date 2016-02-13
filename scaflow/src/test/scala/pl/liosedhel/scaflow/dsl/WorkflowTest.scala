package pl.liosedhel.scaflow.dsl

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow.strategy.FailuresStrategies

import scala.util.Random

class WorkflowTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Workflow DSL" must {
    "run itself" in {
      val resultActor = TestProbe()

      StandardWorkflow.source(List(1, 2, 3))
        .map(a => a * a)
        .group(3)
        .map(_.sum)
        .sink(resultActor.ref).run

      resultActor.expectMsg(14)
    }

    "broadcast messages" in {
      val resultActor1 = TestProbe()
      val resultActor2 = TestProbe()
      StandardWorkflow.source(List(1, 2, 3))
        .map(a => a * a)
        .group(3)
        .map(seq => seq.sum)
        .broadcast(
          StandardWorkflow.connector[Int]().map(a => a * a).map(a => a + 5).sink(resultActor1.ref),
          StandardWorkflow.connector().sink(resultActor2.ref)
        ).run

      resultActor1.expectMsg(14 * 14 + 5)
      resultActor2.expectMsg(14)
    }

    "allow more then one source" in {
      val resultActor1 = TestProbe()
      val resultActor2 = TestProbe()
      val source1 = StandardWorkflow.source(List(1), stopWorkflow = false)
      val source2 = StandardWorkflow.source(List(2, 3))

      val flow = StandardWorkflow.connector[Int]()
        .map(a => a * a)
        .group(3)
        .map(seq => seq.sum)
        .broadcast(
          StandardWorkflow.connector[Int]().map(a => a * a).map(a => a + 5).sink(resultActor1.ref),
          StandardWorkflow.connector().sink(resultActor2.ref)
        ).run

      source1.pipeTo(flow).run
      source2.pipeTo(flow).run

      resultActor1.expectMsg(14 * 14 + 5)
      resultActor2.expectMsg(14)
    }
  }

  "pipe to" in {

    val resultActor = TestProbe()
    val operations = StandardWorkflow.emptyWorkflow[Int]().map(a => a * a)
      .group(3)
      .map(seq => seq.sum)

    StandardWorkflow.source(List(1, 2, 3)).pipeTo(operations).sink(resultActor.ref).run

    resultActor.expectMsg(14)
  }

  "apply custom strategy for map operation" in {

    val resultActor = TestProbe()
    val operations = StandardWorkflow.emptyWorkflow[Int]().map[Int](a => throw new RuntimeException(), Some(FailuresStrategies.OnlyOneRetryStrategy))
      .group(3)
      .map(seq => seq.sum)

    StandardWorkflow.source(List(1, 2, 3)).pipeTo(operations).sink(resultActor.ref).run
    resultActor.expectNoMsg()
  }

  "retry finish job many times" in {
    import scala.concurrent.duration._
    val resultActor = TestProbe()
    val operations = StandardWorkflow.emptyWorkflow[Int]().map[Int](a => if (Random.nextDouble() < 0.8) {
      Thread.sleep(10)
      throw new Exception("Bum")
    } else a * a, Some(FailuresStrategies.RestartAlwaysStrategy), workersNumber = 20)
      .group(3)
      .map(seq => seq.sum, workersNumber = 1)

    StandardWorkflow.source(List(1, 2, 3)).pipeTo(operations).sink(resultActor.ref).run

    resultActor.expectMsg(20.seconds, 14)
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "synchronize flows" in {
    val resultActor = TestProbe()

    val flow1 = StandardWorkflow.source(List(1, 2, 3), stopWorkflow = false)

    val flow2 = StandardWorkflow.source(List(4, 5, 6, 7))

    val synchronizator = StandardWorkflow.synchronization[Int](2).sink(resultActor.ref).run

    flow1.pipeTo(synchronizator).run
    flow2.pipeTo(synchronizator).run

    resultActor.expectMsgPF() { case synchronized: Seq[Int] if synchronized.sorted == List(1, 4) => true }
    resultActor.expectMsgPF() { case synchronized: Seq[Int] if synchronized.sorted == List(2, 5) => true }
    resultActor.expectMsgPF() { case synchronized: Seq[Int] if synchronized.sorted == List(3, 6) => true }
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "synchronize flows by type" in {
    val resultActor = TestProbe()

    val flow1 = StandardWorkflow.source(List(1, 2, 3), stopWorkflow = false)

    val flow2 = StandardWorkflow.source(List("one", "two", "three", "four"))

    val synchronizator = StandardWorkflow.synchronization2[Int, String] {
      case (a: Int, b: String) => (a, b)
      case (a: String, b: Int) => (b, a)
    }.sink(resultActor.ref).run

    flow1.pipeTo(synchronizator).run
    flow2.pipeTo(synchronizator).run

    resultActor.expectMsg((1, "one"))
    resultActor.expectMsg((2, "two"))
    resultActor.expectMsg((3, "three"))
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "split sequence to single elements" in {
    val resultActor = TestProbe()

    val flow = StandardWorkflow.source(List(List(1, 2), List(3), List(4, 5)))
      .split()
      .sink(resultActor.ref)

    flow.run

    resultActor.expectMsg(1)
    resultActor.expectMsg(2)
    resultActor.expectMsg(3)
    resultActor.expectMsg(4)
    resultActor.expectMsg(5)

  }

}

