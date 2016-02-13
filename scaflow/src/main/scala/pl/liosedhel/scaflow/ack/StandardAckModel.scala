package pl.liosedhel.scaflow.ack

import java.util.UUID

import akka.actor.{ Actor, ActorRef, Cancellable }
import pl.liosedhel.scaflow.ack.AckModel.{ EndOfWorkflow, ReliableDelivery }
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed

object StandardAckModel {

  case class Ack(uid: String)
  case class UniqueMessage(uuid: String, message: Any)

  trait StandardAtLeastOnceDelivery[A] extends Actor with EndOfWorkflow with ReliableDelivery {

    import scala.concurrent.duration._

    type UUID = String
    protected def redeliverInterval: FiniteDuration = 300.millis
    private case class TargetAndMessage(target: ActorRef, message: UniqueMessage)

    private var scheduler: Cancellable = _

    private var notConfirmedMessages = Map.empty[UUID, TargetAndMessage]
    private var receivedUUID = Set.empty[UUID]

    import context.dispatcher

    def deliver(target: ActorRef, message: Any): Unit = {
      val uuid = UUID.randomUUID().toString
      val uniqueMessage = UniqueMessage(uuid, message)
      notConfirmedMessages = notConfirmedMessages + (uuid -> TargetAndMessage(target, uniqueMessage))
      target ! UniqueMessage(uuid, message)
      if (scheduler != null) {
        scheduler.cancel()
      }
      scheduleResend()
    }

    private def storeAck(ack: Ack) = {
      notConfirmedMessages = notConfirmedMessages - ack.uid
      if (notConfirmedMessages.isEmpty)
        if (scheduler != null) {
          scheduler.cancel()
        }
    }

    private def sendNotSend(): Unit = {
      notConfirmedMessages.foreach {
        case (key, notConfirmed) => {
          notConfirmed.target ! notConfirmed.message
        }
      }
      scheduleResend()
    }

    private def scheduleResend() = {
      if (context != null)
        scheduler = context.system.scheduler.scheduleOnce(redeliverInterval)(sendNotSend())
    }

    override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
      msg match {
        case UniqueMessage(uuid, AllDataProcessed()) => {
          sender() ! Ack(uuid)
          if (!receivedUUID.contains(uuid)) {
            receivedUUID = receivedUUID + uuid
          }
          allDataProcessedMessageReceived()
          if (notConfirmedMessages.isEmpty) {
            allDataProcessed(context, self)
          }
        }
        case n: UniqueMessage =>
          sender() ! Ack(n.uuid)
          if (!receivedUUID.contains(n.uuid)) {
            receivedUUID = receivedUUID + n.uuid
            receive(n.message)
          }
        case ack: Ack => {
          storeAck(ack)
          if (notConfirmedMessages.isEmpty) {
            allDataProcessed(context, self)
          }
        }

        case _ => super.aroundReceive(receive, msg)
      }
    }

    def checkIfAllDataProcessed() = {
      if (allJobProcessed() && notConfirmedMessages.isEmpty) {
        allDataProcessed(context, self)
      }
    }

  }

}
