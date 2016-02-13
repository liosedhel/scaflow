package pl.liosedhel.scaflow.blocks

import akka.actor.{ Actor, Props }

object DynamicConnectorActor {
  def props(dest: () => Props) = Props(new DynamicConnectorActor(dest))
}

case class DynamicConnectorActor(dest: () => Props, action: Any => Unit = println) extends Actor {
  lazy val destination = context.system.actorOf(dest())

  def receive = {
    case d => {
      action(d)
      destination ! d
    }
  }

}
