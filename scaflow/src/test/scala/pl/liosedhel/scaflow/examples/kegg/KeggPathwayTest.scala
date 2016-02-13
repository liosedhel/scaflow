package pl.liosedhel.scaflow.examples.kegg

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import pl.liosedhel.scaflow.dsl.StandardWorkflow

class KeggPathwayTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Kegg"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  import scala.concurrent.duration._

  "KeggPathway" ignore {

    "find all links for ids using DSL" in {
      val resultActor = TestProbe()
      val keggWorkflow = KeggPathway.createKeggPathwayWorkflowDSL(resultActor.ref)
      StandardWorkflow
        .source(List("hsa"))
        .pipeTo(keggWorkflow)
        .run

      resultActor.expectMsgAnyClassOf(10.minutes, classOf[String])
    }
  }
}