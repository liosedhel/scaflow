package pl.liosedhel.scaflow.blocks.map

import org.scalatest.{ FlatSpec, Matchers }
import pl.liosedhel.scaflow.blocks.map.PullModel.State

class StateTest extends FlatSpec with Matchers {

  "A State" should "add properly new workers" in {
    //GIVEN
    val state = new State[Worker, String]()
    val worker1 = Worker("1")
    val worker2 = Worker("2")

    //WHEN
    state.addWorker(worker1)
    state.addWorker(worker2)

    //THEN
    state.getAllFreeWorkers.size shouldBe 2
  }

  "A State" should "add properly new jobs" in {
    //GIVEN
    val state = new State[Worker, String]()
    val worker1 = Worker("1")
    val job1 = "JOB1"
    val job2 = "JOB2"
    state.addWorker(worker1)

    //WHEN
    state.addWork(job1)
    state.addWork(job2)

    //THEN
    state.workToDo.size shouldBe 2
    state.isJobPending shouldBe true

  }

  "A State" should "assign jobs for workers" in {
    //GIVEN
    val state = new State[Worker, String]()
    val worker1 = Worker("1")
    val job1 = "JOB1"
    state.addWorker(worker1)
    state.addWork(job1)

    //WHEN
    val jobOpt = state.createJobFor(worker1)

    //THEN
    jobOpt.isDefined shouldBe true
    jobOpt.get shouldBe job1
    state.workToDo.size shouldBe 0
    state.isJobPending shouldBe false

    //WHEN
    val jobOptNotDone = state.createJobFor(worker1)

    //THEN
    jobOptNotDone.isDefined shouldBe true
    jobOptNotDone.get shouldBe job1
    state.workToDo.size shouldBe 0
    state.isJobPending shouldBe false

    //WHEN
    state.workDone(worker1, job1)
    state.createJobFor(worker1).isEmpty shouldBe true
    state.isJobPending shouldBe false

  }

  "A State" should "assign many jobs for many workers" in {
    //GIVEN
    val state = new State[Worker, String]()
    val worker1 = Worker("1")
    val worker2 = Worker("2")
    val job1 = "JOB1"
    val job2 = "JOB2"
    state.addWorker(worker1)
    state.addWorker(worker2)
    state.addWork(job1)
    state.addWork(job2)

    //WHEN
    val jobOpt = state.createJobFor(worker1)

    //THEN
    jobOpt.isDefined shouldBe true
    jobOpt.get shouldBe job1
    state.workToDo.size shouldBe 1
    state.workToDo.head shouldBe job2
    state.isJobPending shouldBe true

    //WHEN
    state.workerJobMap.size shouldBe 2
    val jobOpt2 = state.createJobFor(worker2)
    jobOpt2.isDefined shouldBe true
    jobOpt2.get shouldBe job2

    state.isJobPending shouldBe false
  }

  case class Worker(id: String)

}
