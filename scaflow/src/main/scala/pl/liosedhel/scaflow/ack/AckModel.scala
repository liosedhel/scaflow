package pl.liosedhel.scaflow.ack

import akka.actor._
import pl.liosedhel.scaflow.blocks.SourceActor.AllDataProcessed

object AckModel {

  trait EndOfWorkflow extends EndOfWorkflowTrait with ReliableDelivery {

    private var receivedAllDataProcessedMessage = false
    private var waitingForAllDataProcessedAck = false

    /**
     * Forward AllDataProcessed() message and wait for ACK for this message
     * @param context
     */
    def forwardAllDataProcessedMessage(context: ActorContext): Unit = {
      waitingForAllDataProcessedAck = true
      destinations.foreach(deliver(_, AllDataProcessed()))
    }

    /**
     * Marked from AckTrait, when AllDataProcessed() message arrived from previous workflow element
     */
    override def allDataProcessedMessageReceived(): Unit = {
      receivedAllDataProcessedMessage = true
    }

    /**
     * Forward AllDataProcessed() message and
     * try to stop actor if all messages have been processed
     * @param context
     * @param self
     */
    override def allDataProcessed(context: ActorContext, self: ActorRef): Unit = {
      if (allJobProcessed()) {
        if (receivedAllDataProcessedMessage && waitingForAllDataProcessedAck) {
          context.stop(self)
        } else if (receivedAllDataProcessedMessage) {
          forwardAllDataProcessedMessage(context)
        }
      }
    }
  }

  trait EndOfWorkflowTrait {
    def allDataProcessedMessageReceived(): Unit
    def allDataProcessed(context: ActorContext, self: ActorRef)

    /**
     * In case of custom concurrent job transforming or if additional waiting is needed before closeing Workflow
     * @return
     */
    def allJobProcessed(): Boolean = true
  }

  trait ReliableDelivery {
    def deliver(target: ActorRef, message: Any): Unit
    def destinations: Seq[ActorRef]
  }

}
