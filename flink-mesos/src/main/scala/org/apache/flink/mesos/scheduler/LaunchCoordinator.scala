package org.apache.flink.mesos.scheduler

import akka.actor.{Actor, ActorRef, LoggingFSM, Props}
import com.netflix.fenzo._
import com.netflix.fenzo.functions.Action1
import com.netflix.fenzo.plugins.VMLeaseObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.LaunchCoordinator._
import org.apache.flink.mesos.scheduler.messages._
import org.apache.mesos.Protos.TaskInfo
import org.apache.mesos.{MesosSchedulerDriver, Protos}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.duration._

/**
  * The launch coordinator handles offer processing, including
  * matching offers to tasks and making reservations.
  *
  * The coordinator uses Netflix Fenzo to optimize task placement.   During the GatheringOffers phase,
  * offers are evaluated by Fenzo for suitability to the planned tasks.   Reservations are then placed
  * against the best offers, leading to revised offers containing reserved resources with which to launch task(s).
  */
class LaunchCoordinator(
    manager: ActorRef,
    config: Configuration,
    schedulerDriver: MesosSchedulerDriver,
    optimizerBuilder: TaskSchedulerBuilder
  ) extends Actor with LoggingFSM[TaskState, GatherData] {

  /**
    * The task placement optimizer.
    *
    * The optimizer contains the following state:
    *  - unused offers
    *  - existing task placement (for fitness calculation involving task colocation)
    */
  private[mesos] val optimizer: TaskScheduler = {
    optimizerBuilder
      .withLeaseRejectAction(new Action1[VirtualMachineLease]() {
        def call(lease: VirtualMachineLease) {
          schedulerDriver.declineOffer(lease.getOffer.getId)
        }
      }).build
  }

  override def postStop(): Unit = {
    optimizer.shutdown()
    super.postStop()
  }

  /**
    * Initial state
    */
  startWith(Suspended, GatherData(tasks = Nil, newOffers = Nil))

  /**
    * State: Suspended
    *
    * Wait for (re-)connection to Mesos.   No offers exist in this state, but outstanding tasks might.
    */
  when(Suspended) {
    case Event(msg: Connected, data: GatherData) =>
      if(data.tasks.nonEmpty) goto(GatheringOffers)
      else goto(Idle)
  }

  /**
    * State: Idle
    *
    * Wait for a task request to arrive, then transition into gathering offers.
    */
  onTransition {
    case _ -> Idle => assert(nextStateData.tasks.isEmpty)
  }

  when(Idle) {
    case Event(msg: Disconnected, data: GatherData) =>
      goto(Suspended)

    case Event(offers: ResourceOffers, data: GatherData) =>
      // decline any offers that come in
      schedulerDriver.suppressOffers()
      for(offer <- offers.offers().asScala) { schedulerDriver.declineOffer(offer.getId) }
      stay()

    case Event(msg: Launch, data: GatherData) =>
      goto(GatheringOffers) using data.copy(tasks = data.tasks ++ msg.tasks.asScala)
  }

  /**
    * Transition logic to control the flow of offers.
    */
  onTransition {
    case _ -> GatheringOffers =>
      schedulerDriver.reviveOffers()

    case GatheringOffers -> _ =>
      // decline any outstanding offers and suppress future offers
      assert(nextStateData.newOffers.isEmpty)
      schedulerDriver.suppressOffers()
      optimizer.expireAllLeases()
  }

  /**
    * State: GatheringOffers
    *
    * Wait for offers to accumulate for a fixed length of time or from specific slaves.
    *
    * While gathering offers, other task requests may safely arrive.
    */
  when(GatheringOffers, stateTimeout = GATHER_DURATION) {

    case Event(msg: Disconnected, data: GatherData) =>
      // reconciliation spec: offers are implicitly declined upon disconnect
      goto(Suspended) using data.copy(newOffers = Nil)

    case Event(offers: ResourceOffers, data: GatherData) =>
      stay using data.copy(newOffers = data.newOffers ++ offers.offers().asScala)

    case Event(StateTimeout, data: GatherData) =>
      val remaining = MutableMap(data.tasks.map(spec => spec.taskRequest.getId -> spec):_*)

      // attempt to assign the outstanding tasks using the optimizer
      val result = optimizer.scheduleOnce(
        data.tasks.map(_.taskRequest).asJava, data.newOffers.map(new VMLeaseObject(_).asInstanceOf[VirtualMachineLease]).asJava)
      log.debug(result.toString)

      for((hostname, assignments) <- result.getResultMap.asScala) {

        // process the assignments into a set of operations (reserve and/or launch)
        val slaveId = assignments.getLeasesUsed.get(0).getOffer.getSlaveId
        val offerIds = assignments.getLeasesUsed.asScala.map(_.getOffer.getId)
        val operations = processAssignments(slaveId, assignments, remaining.toMap)

        // update the state to reflect the launched tasks
        val launchedTasks = operations
          .filter(_.getType==Protos.Offer.Operation.Type.LAUNCH)
          .flatMap(_.getLaunch.getTaskInfosList.asScala.map(_.getTaskId))
        for(taskId <- launchedTasks) {
          val task = remaining.remove(taskId.getValue).get
          optimizer.getTaskAssigner.call(task.taskRequest, hostname)
        }

        // send the operations to Mesos via manager
        manager ! new AcceptOffers(hostname, offerIds.asJava, operations.asJava)
      }

      // stay in GatheringOffers state if any tasks remain, otherwise transition to idle
      if(remaining.isEmpty) {
        goto(Idle) using data.copy(newOffers = Nil, tasks = Nil)
      } else {
        stay using data.copy(newOffers = Nil, tasks = remaining.values.toList) forMax SUBSEQUENT_GATHER_DURATION
      }
  }

  /**
    * Default handling of events.
    */
  whenUnhandled {
    case Event(msg: Launch, data: GatherData) =>
      // accumulate any tasks that come in
      stay using data.copy(tasks = data.tasks ++ msg.tasks.asScala)

    case Event(offer: OfferRescinded, data: GatherData) =>
      // forget rescinded offers
      optimizer.expireLease(offer.offerId().getValue)
      stay using data.copy(newOffers = data.newOffers.filterNot(_.getId == offer.offerId()))

    case Event(msg: Unassign, _) =>
      // planning to terminate a task - unassign it from its host in the optimizer's state
      optimizer.getTaskUnAssigner.call(msg.taskID.getValue, msg.hostname)
      stay()
  }

  initialize()
}

object LaunchCoordinator {

  val GATHER_DURATION = 5.seconds
  val SUBSEQUENT_GATHER_DURATION = 5.seconds

  // ------------------------------------------------------------------------
  //  FSM State
  // ------------------------------------------------------------------------

  /**
    * An FSM state of the launch coordinator.
    */
  sealed trait TaskState
  case object GatheringOffers extends TaskState
  case object Idle extends TaskState
  case object Suspended extends TaskState

  /**
    * FSM state data.
    * @param tasks the tasks to launch.
    * @param newOffers new offers not yet handed to the optimizer.
    */
  case class GatherData(tasks: Seq[TaskSpecification] = Nil, newOffers: Seq[Protos.Offer] = Nil)

  /**
    * Specifies the task requirements and Mesos task information.
    */
  case class TaskSpecification(taskRequest: TaskRequest, taskInfo: TaskInfo.Builder)

  // ------------------------------------------------------------------------
  //  Messages
  // ------------------------------------------------------------------------

  /**
    * Instructs the launch coordinator to launch some new task.
    */
  case class Launch(tasks: java.util.List[TaskSpecification]) {
    require(tasks.size() >= 1, "Launch message must contain at least one task")
  }

  /**
    * Informs the launch coordinator that some task is no longer assigned to a host (for planning purposes).
    * @param taskID the task ID.
    * @param hostname the hostname.
    */
  case class Unassign(taskID: Protos.TaskID, hostname: String)

  // ------------------------------------------------------------------------
  //  Utils
  // ------------------------------------------------------------------------

  /**
    * Process the given task assignments into a set of Mesos operations.
    *
    * The operations may include reservations and task launches.
    *
    * @param slaveId the slave associated with the given assignments.
    * @param assignments the task assignments as provided by the optimizer.
    * @param allTasks all known tasks, keyed by taskId.
    * @return the operations to perform.
    */
  private def processAssignments(
      slaveId: Protos.SlaveID,
      assignments: VMAssignmentResult,
      allTasks: Map[String, TaskSpecification]): Seq[Protos.Offer.Operation] = {

    def taskInfo(assignment: TaskAssignmentResult): Protos.TaskInfo = {
      allTasks(assignment.getTaskId).taskInfo
        .clone()
        .setSlaveId(slaveId)
        .addResources(scalar("cpus", assignment.getRequest.getCPUs))
        .addResources(scalar("mem", assignment.getRequest.getMemory))
        //.addResources(scalar("disk", assignment.getRequest.getDisk).setRole("Flink"))
        // TODO ports
        .build()
    }

    val launches = Protos.Offer.Operation.newBuilder().setType(Protos.Offer.Operation.Type.LAUNCH).setLaunch(
      Protos.Offer.Operation.Launch.newBuilder().addAllTaskInfos(
        assignments.getTasksAssigned.asScala.map(taskInfo).asJava
      )
    ).build()

    Seq(launches)
  }

  def scalar(name: String, value: Double) = {
    Protos.Resource.newBuilder().setName(name).setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
  }

  def range(name: String, range: Range*) = {
    val values = range.filter(_.step == 1).map(r => Protos.Value.Range.newBuilder().setBegin(r.start).setEnd(r.end).build())
    val ranges = Protos.Value.Ranges.newBuilder().addAllRange(values.asJava).build()
    Protos.Resource.newBuilder().setName(name).setType(Protos.Value.Type.RANGES).setRanges(ranges)
  }

  /**
    * Get the configuration properties for the launch coordinator.
    *
    * @param actorClass the launch coordinator actor class.
    * @param flinkConfig the Flink configuration.
    * @param schedulerDriver the Mesos scheduler driver.
    * @tparam T the launch coordinator actor class.
    * @return the Akka props to create the launch coordinator actor.
    */
  def createActorProps[T <: LaunchCoordinator](
    actorClass: Class[T],
    manager: ActorRef,
    flinkConfig: Configuration,
    schedulerDriver: MesosSchedulerDriver,
    optimizerBuilder: TaskSchedulerBuilder): Props = {

    Props.create(actorClass, manager, flinkConfig, schedulerDriver, optimizerBuilder)
  }
}
