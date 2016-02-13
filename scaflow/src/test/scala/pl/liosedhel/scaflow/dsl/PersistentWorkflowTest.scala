package pl.liosedhel.scaflow.dsl

import akka.actor.{ AddressFromURIString, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.PersistentWorkflowTests
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow_common.operations.ArithmeticOperations

import scala.concurrent.duration._
import scala.util.Random

class PersistentWorkflowTest(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll
    with PersistentWorkflowTests {

  def this() = this(ActorSystem("Persistentflow"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "persistent workflow test" in {
    val resultActor = TestProbe()

    val workflow = PersistentWorkflow.emptyWorkflow[Int]()
      .mapWithOrder(uniqueId("square"), ArithmeticOperations.Pow)
      .group(uniqueId("group"), 3)
      .mapWithOrder(uniqueId("sum"), ArithmeticOperations.Sum)
      .sink(resultActor.ref)

    PersistentWorkflow.source(uniqueId("source1"), List(1, 2)).pipeTo(workflow.run).run
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
    PersistentWorkflow.source(uniqueId("source2"), List(3)).pipeTo(workflow.run).run
    resultActor.expectMsg(14)
  }

  "persistent workflow test fast pull" in {
    val resultActor = TestProbe()

    val workflow = PersistentWorkflow.connector[Int](uniqueId("connector1"))
      .map(uniqueId("square2fast"), ArithmeticOperations.Pow, workersNumber = 4)
      .group(uniqueId("group2fast"), 3)
      .map(uniqueId("sum2fast"), ArithmeticOperations.Sum, workersNumber = 4).sink(resultActor.ref)

    PersistentWorkflow.source(uniqueId("source3"), List(1, 2)).pipeTo(workflow.run).run
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
    PersistentWorkflow.source(uniqueId("source4"), List(3)).pipeTo(workflow.run).run
    resultActor.expectMsg(14)
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "persistent workflow test fast pull remotely" ignore {
    val resultActor = TestProbe()

    val remoteAddress = Seq(AddressFromURIString("akka.tcp://workersActorSystem@localhost:5150"))

    val workflow = PersistentWorkflow.connector[Int](uniqueId("connector1"))
      .map(uniqueId("square2fast"), ArithmeticOperations.Pow, workersNumber = 4, addresses = remoteAddress)
      .group(uniqueId("group2fast"), 3)
      .map(uniqueId("sum2fast"), ArithmeticOperations.Sum, workersNumber = 4).sink(resultActor.ref)

    PersistentWorkflow.source(uniqueId("source3"), List(1, 2)).pipeTo(workflow.run).run
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
    PersistentWorkflow.source(uniqueId("source4"), List(3)).pipeTo(workflow.run).run
    resultActor.expectMsg(14)
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "persistent workflow massive test fast pull" in {
    val resultActor = TestProbe()

    val workflow = PersistentWorkflow.emptyWorkflow[Int]()
      .map(uniqueId("square3fast"), a => if (Random.nextDouble() < 0.8) throw new Exception("Bum") else a, workersNumber = 4)
      .group(uniqueId("group3fast"), 100)
      .map(uniqueId("sum3fast"), seq => seq.sum)

    PersistentWorkflow.source(uniqueId("source5"), 1.to(100)).pipeTo(workflow).sink(resultActor.ref).run
    resultActor.expectMsg(30.seconds, 5050)
  }

  "run at least once delivery map operation" in {
    val resultActor = TestProbe()

    PersistentWorkflow.source(uniqueId("source6"), List(1, 2, 3))
      .mapWithOrder(uniqueId("square"), a => a * a)
      .group(uniqueId("group"), 3)
      .mapWithOrder(uniqueId("sum"), seq => seq.sum)
      .sink(resultActor.ref).run

    resultActor.expectMsg(14)
  }

  "after restart do not sent data once more" in {
    val resultActor = TestProbe()

    val source = uniqueId("source")
    val square = uniqueId("square")
    val group = uniqueId("group")
    val sum = uniqueId("sum")

    val flow = PersistentWorkflow.source(source, List(1, 2, 3))
      .mapWithOrder(square, a => a * a)
      .group(group, 3)
      .mapWithOrder(sum, _.sum)
      .sink(resultActor.ref)

    flow.run

    resultActor.expectMsg(14)
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()

    flow.run
    resultActor.expectNoMsg()
  }

  "split sequence to single elements" in {
    val resultActor = TestProbe()

    val source = uniqueId("source")
    val splitter = uniqueId("splitter")

    val flow = PersistentWorkflow.source(source, List(List(1, 2), List(3), List(4, 5)))
      .split(splitter)
      .sink(resultActor.ref)

    flow.run

    resultActor.expectMsg(1)
    resultActor.expectMsg(2)
    resultActor.expectMsg(3)
    resultActor.expectMsg(4)
    resultActor.expectMsg(5)

    //flow.run
    resultActor.expectMsg(AllDataProcessed())
    resultActor.expectNoMsg()
  }

  "advanced strategy applaying" in {
    val resultActor = TestProbe()

    val workflow = PersistentWorkflow.emptyWorkflow[Int]()
      .map(uniqueId("square3fast"), a => if (Random.nextDouble() < 0.8) throw new Exception("Bum") else a, workersNumber = 4)
      .group(uniqueId("group3fast"), 100)
      .map(uniqueId("sum3fast"), seq => seq.sum)

    PersistentWorkflow.source(uniqueId("source5"), 1.to(100)).pipeTo(workflow).sink(resultActor.ref).run
    resultActor.expectMsg(30.seconds, 5050)
  }

  "advanced strategy applaying remotely" ignore {
    val resultActor = TestProbe()

    val addresses = Seq(AddressFromURIString("akka.tcp://workersActorSystem@localhost:5150"))

    val workflow = PersistentWorkflow.emptyWorkflow[Int]()
      .map(uniqueId("square3fast"), ArithmeticOperations.BuggyTransformation, workersNumber = 4, addresses = addresses)
      .group(uniqueId("group3fast"), 100)
      .map(uniqueId("sum3fast"), seq => seq.sum)

    PersistentWorkflow.source(uniqueId("source5"), 1.to(100)).pipeTo(workflow).sink(resultActor.ref).run
    resultActor.expectMsg(30.seconds, 5050)
  }

}
