package pl.liosedhel.scaflow.examples.kegg

import java.io._
import java.net.URL
import javax.imageio.ImageIO

import akka.actor.{ ActorRef, ActorSystem }
import pl.liosedhel.scaflow.blocks._
import pl.liosedhel.scaflow.blocks.map.MapMasterActor
import pl.liosedhel.scaflow.dsl.StandardWorkflow
import pl.liosedhel.scaflow.strategy.FailuresStrategies

import scala.io.BufferedSource

object KeggPathway {
  def pathwayUrl(id: String) = s"http://rest.kegg.jp/list/pathway/$id"
  def getPathwayDetailsUrl(id: String) = s"http://rest.kegg.jp/get/$id"
  def getMapPathwayMapPngUrl(id: String) = s"http://rest.kegg.jp/get/$id/image"

  def downloadFile(id: String, extension: String): String = {
    val src = new URL(getMapPathwayMapPngUrl(id))
    val outputFile = new File(s"pathways/$id.$extension")
    ImageIO.write(ImageIO.read(src), "png", outputFile)
    id
  }

  def saveStringToFile(id: String, text: String): String = {
    val out = new java.io.FileWriter(s"pathways/$id.txt")
    out.write(text)
    out.close()
    id
  }

  def downloadData(httpUrl: String): Iterator[String] = {
    val url = new URL(httpUrl)
    val is = url.openStream()
    val br = new BufferedSource(is)
    val lines = br.getLines()
    is.close()
    lines
  }

  def getSetOfPathways(id: String)(implicit actorSystem: ActorSystem): List[String] = {
    println(id)
    val result = downloadData(pathwayUrl(id)).map(_.split("\t")(0)).map(_.split(":")(1)).toList
    println("Liczba ścieżek pobranych:" + result.size)
    result
  }

  def getPathwayDetails(id: String)(implicit actorSystem: ActorSystem): String = {
    saveStringToFile(id, downloadData(getPathwayDetailsUrl(id)).mkString("\n"))
  }

  def getPathwayMapPng(id: String)(implicit actorSystem: ActorSystem): String = {
    downloadFile(id, "png")
  }

  class UniqueDataListFilter[A] extends (A => Boolean) {
    var uniqueSet = Set.empty[A]

    override def apply(data: A): Boolean = {
      val result = !uniqueSet.contains(data)
      uniqueSet += data
      result
    }
  }

  def run(identifiers: List[String])(implicit actorSystem: ActorSystem): Unit = {
    lazy val pathwayFlow = actorSystem.actorOf(MapMasterActor.props(getSetOfPathways, filterGens, None, 4, Seq.empty))
    lazy val filterGens = actorSystem.actorOf(FilterActor.props(splitter, new UniqueDataListFilter[String]))
    lazy val splitter = actorSystem.actorOf(SplitterActor.props(entryForGen))
    lazy val entryForGen = actorSystem.actorOf(MapMasterActor.props(getPathwayDetails, sink, None, 4, Seq.empty))
    lazy val sink = actorSystem.actorOf(SinkActor.props(println))

    lazy val source = actorSystem.actorOf(SourceActor.props(pathwayFlow, identifiers, stopWorkflow = true))
    SourceActor.run(source)

  }

  def createKeggPathwayWorkflowDSL(sinkActor: ActorRef)(implicit actorSystem: ActorSystem) = {

    val savePathwayPngFlow = StandardWorkflow.connector[String]().map(getPathwayMapPng)
      .map(id => s"PNG map downloaded for $id").pipeTo(sinkActor)
    val savePathwayTextFlow = StandardWorkflow.connector[String]().map(getPathwayDetails)
      .map(id => s"TXT details download for pathway $id").pipeTo(sinkActor)

    StandardWorkflow.emptyWorkflow()
      .map(getSetOfPathways, Some(FailuresStrategies.HTTPSupervisorStrategy))
      .split[String]()
      .broadcast(savePathwayPngFlow, savePathwayTextFlow)
  }
}

