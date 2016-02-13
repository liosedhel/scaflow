package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.PersistentActor
import pl.liosedhel.scaflow.ack.PersistentAckModel.PersistentAtLeastOnceDelivery
import pl.liosedhel.scaflow.ack.StandardAckModel.StandardAtLeastOnceDelivery
import pl.liosedhel.scaflow.blocks.GroupActor.{ DefaultGroupStrategy, GroupStrategy }
import pl.liosedhel.scaflow_common.model.{ Next, NextVal }

object GroupActor {
  @SerialVersionUID(1L)
  case class Grouped[A](id: Int, data: Seq[A]) extends NextVal[Seq[A]]

  def props[A](destination: ActorRef, strategy: GroupStrategy[A] = new DefaultGroupStrategy[A](2)) = Props(new GroupActor(destination, strategy))
  def props[A](destination: ActorRef, length: Int) = Props(new GroupActor(destination, new DefaultGroupStrategy[Any](length)))

  trait GroupStrategy[A] {
    def group(next: NextVal[A]): Option[Grouped[A]]
  }

  class DefaultGroupStrategy[A](length: Int) extends GroupStrategy[A] {
    var dataSeq = Seq.empty[A]
    var seq = 0

    override def group(next: NextVal[A]): Option[Grouped[A]] = {

      val tmpDataSeq = dataSeq :+ next.data

      if (tmpDataSeq.size == length) {
        seq += 1
        dataSeq = Seq.empty[A]
        Some(Grouped(seq, tmpDataSeq))
      } else {
        dataSeq = tmpDataSeq
        None
      }

    }
  }

  class GroupByKeyId[A](length: Int) extends GroupStrategy[A] {
    var groupedItemMap = Map.empty[Int, List[(Int, A)]]

    override def group(next: NextVal[A]): Option[Grouped[A]] = {
      val key = next.id / length
      val groupedItems = groupedItemMap.get(key).fold(List((next.id % length, next.data)))(groupedList => groupedList :+ (next.id % length, next.data))
      groupedItemMap = groupedItemMap + (key -> groupedItems)
      if (groupedItems.size == length) {
        groupedItemMap = groupedItemMap - key
        Some(Grouped(key, groupedItems.sortBy(key * length + _._1).map(_._2)))
      } else {
        None
      }
    }
  }
}

case class GroupActor[A](destination: ActorRef, strategy: GroupStrategy[A])
    extends Actor with StandardAtLeastOnceDelivery[Seq[A]] {

  def receive = {
    case n: NextVal[A] =>
      strategy.group(n).foreach(grouped => deliver(destination, grouped))
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}

object GroupPersistentActor {
  def props[A](id: String, destination: ActorRef, strategy: GroupStrategy[A] = new DefaultGroupStrategy[A](2)) = Props(new GroupPersistentActor(id, destination, strategy))
  def props[A](id: String, destination: ActorRef, length: Int) = Props(new GroupPersistentActor(id, destination, new DefaultGroupStrategy[Any](length)))
}

case class GroupPersistentActor[A](id: String,
  destination: ActorRef,
  strategy: GroupStrategy[A])
    extends PersistentActor
    with PersistentAtLeastOnceDelivery {

  override def receiveRecover: Receive = receiveRecoverWithAck {
    case n: NextVal[A] =>
      strategy.group(n).foreach(n => deliver(destination, n))
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = receiveCommandWithAck {
    case n: NextVal[A] =>
      persistAsync(n) { e =>
        strategy.group(e).foreach(n => deliver(destination, Next(n.id, n.data)))
      }
  }

  override def destinations: Seq[ActorRef] = Seq(destination)
}
