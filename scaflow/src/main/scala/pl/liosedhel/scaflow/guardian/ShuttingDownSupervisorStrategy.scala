package pl.liosedhel.scaflow.guardian

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ AllForOneStrategy, OneForOneStrategy, SupervisorStrategy }

class ShuttingDownSupervisorStrategy extends akka.actor.SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = AllForOneStrategy() { case _ => Escalate }
}
