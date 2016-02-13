package pl.liosedhel.scaflow.examples.dsl

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.PersistentWorkflowTests
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow.dsl.PersistentWorkflow

class PersistentWorkflowExamples(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll
    with PersistentWorkflowTests {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Persistent Workflow DSL" must {
    "be able to parallel split" in {
      //parallel split
      val resultActor1 = TestProbe()
      val resultActor2 = TestProbe()
      val source1 = PersistentWorkflow.source(uniqueId("source"), List(1, 2, 3))
      val flow = PersistentWorkflow.connector[Int](uniqueId("connector2"))
        .map(uniqueId("square1"), a => a * a)
        .group(uniqueId("group1"), 3)
        .map(uniqueId("sum1"), seq => seq.sum)
        .broadcast(uniqueId("broadcast1"),
          PersistentWorkflow.connector[Int](uniqueId("connector3")).map(uniqueId("square2"), a => a * a).map(uniqueId("add2"), a => a + 5).sink(resultActor1.ref),
          PersistentWorkflow.connector(uniqueId("connector4")).sink(resultActor2.ref)
        )

      source1.pipeTo(flow).run

      resultActor1.expectMsg(14 * 14 + 5)
      resultActor2.expectMsg(14)
    }

    "be able to simple merge" in {
      //simple merge, multi merge
      val testProbe = TestProbe()
      val runningFlow3 = PersistentWorkflow.connector[Int](uniqueId("connector2"))
        .map(uniqueId("println"), identity)
        .sink(testProbe.ref)
        .run

      val flow1 = PersistentWorkflow.source[Int](uniqueId("mergeSource1"), List(1, 2, 3))
        .map(uniqueId("mergeSquare"), a => a * a)

      val flow2 = PersistentWorkflow.source[Int](uniqueId("mergeSource2"), List(1, 2, 3))
        .map(uniqueId("mergeSum"), _ + 5)

      flow2.pipeTo(runningFlow3).run
      flow1.pipeTo(runningFlow3).run

      testProbe.expectMsgAllClassOf(classOf[Int])

    }

    "exclusive choice" in {
      val evenNumbersResult = TestProbe()
      val oddNumbersResult = TestProbe()
      val source1 = PersistentWorkflow.source(uniqueId(), List(1, 2))

      val flow = PersistentWorkflow.connector[Int](uniqueId())
        .broadcast(uniqueId(),
          PersistentWorkflow.connector[Int](uniqueId()).filter(uniqueId(), _ % 2 == 0).sink(evenNumbersResult.ref),
          PersistentWorkflow.connector[Int](uniqueId()).filter(uniqueId(), _ % 2 == 1).sink(oddNumbersResult.ref)
        )

      source1.pipeTo(flow).run

      evenNumbersResult.expectMsg(2)
      oddNumbersResult.expectMsg(1)

      evenNumbersResult.expectMsg(AllDataProcessed())
      oddNumbersResult.expectMsg(AllDataProcessed())
      evenNumbersResult.expectNoMsg()
      oddNumbersResult.expectNoMsg()
    }
    //others maybe possible
    // http://www.workflowpatterns.com/patterns/control/multiple_instance/wcp12.php via new `spawn` method

  }

}