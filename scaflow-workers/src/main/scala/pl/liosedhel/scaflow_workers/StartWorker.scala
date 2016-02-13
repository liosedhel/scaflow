package pl.liosedhel.scaflow_workers

import akka.actor.ActorSystem

object StartWorker extends App {
  val workerActorSystem = ActorSystem("workersActorSystem")

}
