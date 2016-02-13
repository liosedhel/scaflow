package pl.liosedhel.scaflow.strategy

import akka.actor.SupervisorStrategy._
import akka.actor._

import scala.concurrent.duration._

object FailuresStrategies {

  trait DropMessage {
    def dropMessage()
  }

  case class OverridenSupervisorStrategy(dropMessage: DropMessage)(supervisor: SupervisorStrategy) extends SupervisorStrategy {

    override def decider: Decider = supervisor.decider.andThen({
      case Stop =>
        dropMessage.dropMessage(); Restart
      case rest => rest
    })

    override def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = {
      supervisor.handleChildTerminated(context, child, children)
    }

    override def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Unit = {
      supervisor.processFailure(context, restart, child, cause, stats, children)
    }
  }

  val HTTPSupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 20.seconds) {
      case _: RuntimeException => Restart
      case _: java.util.concurrent.TimeoutException => Restart
      case _: java.net.UnknownHostException => Stop
      case _ => Escalate
    }

  val OnlyOneRetryStrategy = AllForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1.minute) {
    case _: RuntimeException => Restart
    case _ => Escalate
  }

  val RestartAlwaysStrategy = OneForOneStrategy(maxNrOfRetries = Int.MaxValue, withinTimeRange = 1.seconds) {
    case _ => Restart
  }
}
