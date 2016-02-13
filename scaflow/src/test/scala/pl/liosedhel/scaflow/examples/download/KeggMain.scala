package pl.liosedhel.scaflow.examples.download

import java.io.File
import java.net.URL
import javax.imageio.ImageIO

import akka.actor.SupervisorStrategy.{ Restart, Stop }
import akka.actor.{ ActorSystem, OneForOneStrategy }
import pl.liosedhel.scaflow.dsl.{ PersistentWorkflow, StandardWorkflow }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, TimeoutException }
import scala.io.BufferedSource

object KeggMain extends App {

  def pathwayUrl(id: String) = s"http://rest.kegg.jp/list/pathway/$id"
  def getPathwayDetailsUrl(id: String) = s"http://rest.kegg.jp/get/$id"
  def getMapPathwayMapPngUrl(id: String) = s"http://rest.kegg.jp/get/$id/image"

  def downloadFile(id: String, extension: String)(implicit context: ExecutionContext): String = {
    val src = new URL(getMapPathwayMapPngUrl(id))
    val outputFile = new File(s"pathways/$id.$extension")
    Await.result(Future { ImageIO.write(ImageIO.read(src), "png", outputFile) }, 30.seconds)
    id
  }

  def saveStringToFile(id: String, text: String): String = {
    val out = new java.io.FileWriter(s"pathways/$id.txt")
    out.write(text)
    out.close()
    id
  }

  def downloadData(httpUrl: String)(implicit context: ExecutionContext): List[String] = {
    val url = new URL(httpUrl)
    val is = url.openStream()
    val br = new BufferedSource(is)
    Await.result(Future { br.getLines().toList }, 30.seconds)
  }

  def getSetOfPathways(id: String)(implicit actorSystem: ActorSystem): List[String] = {
    downloadData(pathwayUrl(id))(actorSystem.dispatcher).map(_.split("\t")(0)).map(_.split(":")(1))
  }

  def getPathwayDetails(id: String)(implicit actorSystem: ActorSystem): String = {
    saveStringToFile(id, downloadData(getPathwayDetailsUrl(id))(actorSystem.dispatcher).mkString("\n"))
  }

  def getPathwayMapPng(id: String)(implicit actorSystem: ActorSystem): String = {
    downloadFile(id, "png")(actorSystem.dispatcher)
  }

  import scala.concurrent.duration._

  val HTTPSupervisorStrategy = OneForOneStrategy(100, 1.second) {
    case e: TimeoutException => Restart
    case _ => Restart
  }

  implicit val actorSystem = ActorSystem("kegg")

  def standardWorkflow()(implicit actorSystem: ActorSystem) = {
    val savePathwayPngFlow2 = StandardWorkflow.connector[String]()
      .map(getPathwayMapPng, Some(HTTPSupervisorStrategy))
      .sink(id => println(s"PNG map downloaded for $id"))

    val savePathwayTextFlow2 = StandardWorkflow.connector[String]()
      .map(getPathwayDetails, Some(HTTPSupervisorStrategy))
      .sink(id => println(s"TXT details download for pathway $id"))

    StandardWorkflow
      .source(List("hsa"))
      .map(getSetOfPathways, Some(HTTPSupervisorStrategy))
      .split[String]()
      .broadcast(savePathwayPngFlow2, savePathwayTextFlow2)
      .run
  }

  def persistentWorkflow()(implicit actorSystem: ActorSystem) = {
    val savePathwayPngFlow = PersistentWorkflow.connector[String]("pngConnector")
      .map("getPng", getPathwayMapPng, Some(HTTPSupervisorStrategy))
      .sink("sinkPng", id => println(s"PNG map downloaded for $id"))

    val savePathwayTextFlow = PersistentWorkflow.connector[String]("textConnector")
      .map("getTxt", getPathwayDetails, Some(HTTPSupervisorStrategy))
      .sink("sinkTxt", id => println(s"TXT details download for pathway $id"))

    PersistentWorkflow
      .source("source", List("hsa"))
      .map("getSetOfPathways", getSetOfPathways, Some(HTTPSupervisorStrategy))
      .split[String]("split")
      .broadcast("broadcast", savePathwayPngFlow, savePathwayTextFlow)
      .run
  }
}
