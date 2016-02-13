package pl.liosedhel.scaflow.performance

import akka.actor._
import akka.util.Timeout
import pl.liosedhel.scaflow.PersistentWorkflowTests
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed
import pl.liosedhel.scaflow.dsl.{ FlowElement, PersistentWorkflow, StandardWorkflow }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object PerformanceTest extends App with PersistentWorkflowTests {

  var startTime: Long = System.currentTimeMillis()

  implicit val actorSystem = ActorSystem("performance-test")

  def basicFilter() = StandardWorkflow.connector[Int]().filter(_ < Int.MaxValue)
  def persistentBasicFilter() = {
    PersistentWorkflow.connector[Int](uniqueId()).filter(uniqueId(), _ < Int.MaxValue)
  }
  new StandardPerformanceTestUnit("filter", 1000, basicFilter).run()
  new StandardPerformanceTestUnit("filter", 1000, basicFilter).run()
  new StandardPerformanceTestUnit("filter", 10000, basicFilter).run()
  new StandardPerformanceTestUnit("filter", 50000, basicFilter).run()

  new PersistentPerformanceTestUnit("filter", 1000, persistentBasicFilter).run()
  new PersistentPerformanceTestUnit("filter", 1000, persistentBasicFilter).run()
  new PersistentPerformanceTestUnit("filter", 10000, persistentBasicFilter).run()
  new PersistentPerformanceTestUnit("filter", 50000, persistentBasicFilter).run()
}

class PersistentPerformanceTestUnit(val testName: String,
  val numberOfElements: Int,
  persistentFlow: () => PersistentWorkflow[Int] with FlowElement)
    extends PerformanceTestTrait with PersistentWorkflowTests {

  import akka.pattern.ask
  val flowType = "PersistentFlow"

  def runFlow()(implicit actorSystem: ActorSystem): Long = {
    val timeMeasureActor = actorSystem.actorOf(Props(new PerformanceTestActor))
    startTime = System.currentTimeMillis()
    val fullPersistentWorkflow =
      PersistentWorkflow.source(uniqueId(), 1 to numberOfElements).pipeTo(persistentFlow()).sink(timeMeasureActor)
    val time = Await.result({
      fullPersistentWorkflow.run
      timeMeasureActor.ask(AreYouDone())(timeout)
    }, Duration.Inf).asInstanceOf[YesIAmDone]

    timeMeasureActor ! PoisonPill
    time.time
  }

}

class StandardPerformanceTestUnit(val testName: String,
  val numberOfElements: Int,
  standardFlow: () => StandardWorkflow[Int] with FlowElement)
    extends PerformanceTestTrait with PersistentWorkflowTests {
  import akka.pattern.ask

  val flowType = "StandardFlow"

  def runFlow()(implicit actorSystem: ActorSystem): Long = {
    val timeMeasureActor = actorSystem.actorOf(Props(new PerformanceTestActor))
    startTime = System.currentTimeMillis()
    val fullStandardWorkflow =
      StandardWorkflow.source(1 to numberOfElements).pipeTo(standardFlow()).sink(timeMeasureActor)
    val time = Await.result({
      fullStandardWorkflow.run
      timeMeasureActor.ask(AreYouDone())(timeout)
    }, Duration.Inf).asInstanceOf[YesIAmDone]
    timeMeasureActor ! PoisonPill
    time.time
  }

}

trait PerformanceTestTrait {

  val numberOfElements: Int
  val testName: String
  val flowType: String

  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.minute)

  case class AreYouDone()
  case class YesIAmDone(time: Long)

  class PerformanceTestActor extends Actor {
    var asker: ActorRef = null

    override def receive: Receive = {
      case AllDataProcessed() =>
        asker ! YesIAmDone(System.currentTimeMillis() - startTime)
      case AreYouDone() => asker = sender()
    }
  }

  var startTime: Long = System.currentTimeMillis()

  def run()(implicit actorSystem: ActorSystem) = {
    (1 to 5).map { i =>
      val time = runFlow()
      println(s"$i\t$flowType\t$testName\t$numberOfElements\t$time ")
      time
    }
    println()
  }
  protected def runFlow()(implicit actorSystem: ActorSystem): Long
}
