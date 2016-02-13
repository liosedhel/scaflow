package pl.liosedhel.scaflow.ack

import akka.actor.{ ActorPath, ActorRef }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.AckModel.{ ReliableDelivery, EndOfWorkflow }
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed

object PersistentAckModel {

  case class PersistentUniqueMessage(deliveryId: Long, message: Any)
  case class PersistentAck(deliveryId: Long)
  case class PersistentAckEvent(deliveryId: Long)

  trait PersistentAtLeastOnceDelivery
      extends akka.persistence.AtLeastOnceDelivery with EndOfWorkflow with ReliableDelivery {
    persistentActor: PersistentActor =>

    private var receivedDeliveryIds = Set.empty[(ActorPath, Long)]

    def deliver(destination: ActorRef, n: Any): Unit =
      deliver(destination.path)((deliveryId: Long) => {
        PersistentUniqueMessage(deliveryId, n)
      })

    def receiveCommandWithAck(receiveCommand: Receive): Receive =
      receiveCommand.orElse {
        case PersistentUniqueMessage(deliveryId, AllDataProcessed()) =>
          persistAsync(AllDataProcessed()) { kw =>
            val senderPath = sender().path
            sender() ! PersistentAck(deliveryId)
            if (!receivedDeliveryIds.contains((senderPath, deliveryId))) {
              receivedDeliveryIds += ((senderPath, deliveryId))
            }
            allDataProcessedMessageReceived()
            if (numberOfUnconfirmed == 0) {
              allDataProcessed(context, self)
            }
          }
        case pum @ PersistentUniqueMessage(deliveryId, n) =>
          val senderPath = sender().path
          sender() ! PersistentAck(pum.deliveryId)
          if (!receivedDeliveryIds.contains((senderPath, pum.deliveryId))) {
            receivedDeliveryIds += ((senderPath, pum.deliveryId))
            receiveCommand(n)
          }
        case pAck @ PersistentAck(deliveryId) =>
          persistAsync(PersistentAckEvent(deliveryId)) { p =>
            confirmDelivery(p.deliveryId)
            if (numberOfUnconfirmed == 0) {
              allDataProcessed(context, self)
            }
          }
      }

    def receiveRecoverWithAck(receiveRecover: Receive): Receive =
      receiveRecover.orElse {
        case PersistentAckEvent(deliveryId) => {
          confirmDelivery(deliveryId)
        }
        case AllDataProcessed() => {
          allDataProcessedMessageReceived()
        }
      }

    def tryToStopActor() = {
      if (allJobProcessed() && numberOfUnconfirmed == 0) {
        allDataProcessed(context, self)
      }
    }

  }

}

