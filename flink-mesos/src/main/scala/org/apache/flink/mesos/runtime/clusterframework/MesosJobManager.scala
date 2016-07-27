package org.apache.flink.mesos.runtime.clusterframework

import java.util.concurrent.ExecutorService

import akka.actor.ActorRef
import org.apache.flink.configuration.{Configuration => FlinkConfiguration}
import org.apache.flink.metrics.MetricRegistry
import org.apache.flink.runtime.checkpoint.{CheckpointRecoveryFactory, SavepointStore}
import org.apache.flink.runtime.clusterframework.ContaineredJobManager
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.leaderelection.LeaderElectionService

import scala.concurrent.duration._

/** JobManager actor for execution on Mesos. .
  *
  * @param flinkConfiguration Configuration object for the actor
  * @param executorService Execution context which is used to execute concurrent tasks in the
  *                         [[org.apache.flink.runtime.executiongraph.ExecutionGraph]]
  * @param instanceManager Instance manager to manage the registered
  *                        [[org.apache.flink.runtime.taskmanager.TaskManager]]
  * @param scheduler Scheduler to schedule Flink jobs
  * @param libraryCacheManager Manager to manage uploaded jar files
  * @param archive Archive for finished Flink jobs
  * @param restartStrategyFactory Restart strategy to be used in case of a job recovery
  * @param timeout Timeout for futures
  * @param leaderElectionService LeaderElectionService to participate in the leader election
  */
class MesosJobManager(flinkConfiguration: FlinkConfiguration,
                      executorService: ExecutorService,
                      instanceManager: InstanceManager,
                      scheduler: FlinkScheduler,
                      libraryCacheManager: BlobLibraryCacheManager,
                      archive: ActorRef,
                      restartStrategyFactory: RestartStrategyFactory,
                      timeout: FiniteDuration,
                      leaderElectionService: LeaderElectionService,
                      submittedJobGraphs : SubmittedJobGraphStore,
                      checkpointRecoveryFactory : CheckpointRecoveryFactory,
                      savepointStore: SavepointStore,
                      jobRecoveryTimeout: FiniteDuration,
                      metricsRegistry: Option[MetricRegistry])
  extends ContaineredJobManager(
    flinkConfiguration,
    executorService,
    instanceManager,
    scheduler,
    libraryCacheManager,
    archive,
    restartStrategyFactory,
    timeout,
    leaderElectionService,
    submittedJobGraphs,
    checkpointRecoveryFactory,
    savepointStore,
    jobRecoveryTimeout,
    metricsRegistry) {

  val jobPollingInterval: FiniteDuration = 5 seconds

}
