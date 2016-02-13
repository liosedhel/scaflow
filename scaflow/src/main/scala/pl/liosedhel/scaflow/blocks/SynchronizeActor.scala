package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.SynchronizeActor.Synchronized
import pl.liosedhel.scaflow_common.model.NextVal

object SynchronizeActor {
  case class Synchronized[A](override val id: Int, override val data: A) extends NextVal[A]
  def props[A, B](synchronizationStrategy: SynchronizationStrategy[A, B], destination: ActorRef) = Props(new SynchronizeActor(synchronizationStrategy, destination))
}

trait SynchronizationStrategy[A, B] {
  def synchronize(sender: ActorRef, data: NextVal[A]): Option[Synchronized[B]]
}

class DefaultSynchronizationStrategy[A](sourceNumber: Int) extends SynchronizationStrategy[A, Seq[A]] {
  val sourceDataMap = scala.collection.mutable.Map.empty[ActorRef, Seq[A]]
  var currentId = -1

  override def synchronize(sender: ActorRef, data: NextVal[A]): Option[Synchronized[Seq[A]]] = {
    sourceDataMap += sender -> (sourceDataMap.getOrElse(sender, Seq.empty[A]) :+ data.data)
    if (sourceDataMap.size == sourceNumber) {
      val firstElementsFromEachSource = sourceDataMap.keys.map(s => sourceDataMap.get(s).flatMap(_.headOption))
      val synchronizedOpt = firstElementsFromEachSource.foldLeft(Option(Seq.empty[A]))((seqOpt, a) =>
        if (seqOpt.isDefined && a.isDefined) Some(seqOpt.get :+ a.get) else Option.empty[Seq[A]])

      synchronizedOpt.map { synchronized =>
        currentId += 1
        sourceDataMap.keys.map(s => sourceDataMap.put(s, sourceDataMap.get(s).get.tail))
        Synchronized(currentId, synchronized)
      }
    } else
      None
  }
}

class TypeBasedSynchronization2[A, B](convert: (Any, Any) => (A, B)) extends SynchronizationStrategy[Any, (A, B)] {

  val listActorMap = collection.mutable.Map.empty[ActorRef, List[Any]]

  override def synchronize(sender: ActorRef, data: NextVal[Any]): Option[Synchronized[(A, B)]] = {
    listActorMap.update(sender, listActorMap.getOrElse(sender, List.empty[Any]) :+ data.data)
    val eachListHasElement =
      listActorMap.keys.forall(key => listActorMap.getOrElse(key, List.empty[Any]).nonEmpty)
    if (listActorMap.keys.size == 2 && eachListHasElement) {
      listActorMap.values.map(_.head) match {
        case a :: b :: tail =>
          listActorMap.keys.foreach(key => listActorMap.update(key, listActorMap.get(key).get.tail))
          Some(Synchronized(1, convert(a, b)))
      }
    } else
      None
  }

}

case class SynchronizeActor[A, B](synchronizationStrategy: SynchronizationStrategy[A, B],
  destination: ActorRef)
    extends Actor with StandardAtLeastOnceDelivery[B] {

  override def receive: Receive = {
    case n: NextVal[A] => {
      synchronizationStrategy.synchronize(sender(), n).foreach(message => deliver(destination, message))
    }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}