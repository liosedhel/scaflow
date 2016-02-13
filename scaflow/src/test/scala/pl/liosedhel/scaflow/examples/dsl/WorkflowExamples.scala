package pl.liosedhel.scaflow.examples.dsl

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow.dsl.StandardWorkflow

class WorkflowExamples(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Workflow DSL" must {
    "be able to parallel split" in {
      //parallel split
      val resultActor1 = TestProbe()
      val resultActor2 = TestProbe()
      val source1 = StandardWorkflow.source(List(1, 2, 3))

      val flow = StandardWorkflow.connector[Int]()
        .map(a => a * a)
        .group(3)
        .map(seq => seq.sum)
        .broadcast(
          StandardWorkflow.connector[Int]().map(a => a * a).map(a => a + 5).sink(resultActor1.ref),
          StandardWorkflow.connector().sink(resultActor2.ref)
        )

      source1.pipeTo(flow).run

      resultActor1.expectMsg(14 * 14 + 5)
      resultActor2.expectMsg(14)
    }

    "be able to simple merge" in {
      //simple merge, multi merge
      val testProbe = TestProbe()
      val runningFlow3 = StandardWorkflow.connector()
        .sink(testProbe.ref)
        .run

      val flow1 = StandardWorkflow.source(List(1, 2, 3))
        .map(a => a * a)

      val flow2 = StandardWorkflow.source(List(1, 2, 3))
        .map(a => a + 5)

      flow1.pipeTo(runningFlow3).run
      flow2.pipeTo(runningFlow3).run

      testProbe.expectMsgAllClassOf(classOf[Int])

    }

    "exclusive choice" in {
      val evenNumbersResult = TestProbe()
      val oddNumbersResult = TestProbe()
      val source1 = StandardWorkflow.source(List(1, 2))

      val flow = StandardWorkflow.connector[Int]()
        .broadcast(
          StandardWorkflow.connector[Int]().filter(a => a % 2 == 0).sink(evenNumbersResult.ref),
          StandardWorkflow.connector[Int]().filter(a => a % 2 == 1).sink(oddNumbersResult.ref)
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