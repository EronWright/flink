package org.apache.flink.mesos.dispatcher;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.types.SessionDefaults;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionIDRetrievable;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.scheduler.*;
import org.apache.flink.mesos.scheduler.messages.*;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.ZooKeeperUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.mesos.Utils.uri;
import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_CLASSPATH;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_FLINK_CLASSPATH;

/**
 *
 * Leadership bestows write access to Mesos.
 */
public class MesosDispatcherBackend extends AbstractDispatcherBackend<MesosDispatcherBackend.MesosSession> {

	/** The Mesos configuration (master and framework info) */
	private final MesosConfiguration mesosConfig;

	/** Context information used to start a AppMaster Java process */
	private final Protos.TaskInfo.Builder appMasterTaskInfo;

	/** The persistent session store */
	private final MesosSessionStore sessionStore;

	/** in-memory session state */
	private final SessionState sessionState;

	final Map<ResourceID, MesosSessionStore.Session> mastersInNew;
	final Map<ResourceID, MesosSessionStore.Session> mastersInLaunch;
	final Map<ResourceID, MesosSessionStore.Session> mastersBeingReturned;

	/** Callback handler for the asynchronous Mesos scheduler */
	private SchedulerProxy schedulerCallbackHandler;

	/** Mesos scheduler driver */
	private SchedulerDriver schedulerDriver;

	private ActorRef connectionMonitor;

	private ActorRef taskRouter;

	private ActorRef launchCoordinator;

	private ActorRef reconciliationCoordinator;

	private MesosArtifactServer artifactServer;

	private SessionDefaults sessionDefaults;

	public MesosDispatcherBackend(
		Configuration flinkConfig,
		MesosConfiguration mesosConfig,
		Protos.TaskInfo.Builder appMasterTaskInfo,
		MesosSessionStore sessionStore,
		SessionDefaults sessionDefaults,
		LeaderElectionService leaderElectionService,
		MesosArtifactServer artifactServer,
		Path flinkJar) {

		super(flinkConfig, leaderElectionService);

		this.mesosConfig = requireNonNull(mesosConfig);
		this.appMasterTaskInfo = requireNonNull(appMasterTaskInfo);
		this.sessionStore = requireNonNull(sessionStore);
		this.sessionDefaults = requireNonNull(sessionDefaults);
		this.artifactServer = artifactServer;

		this.mastersInNew = new HashMap<>();
		this.mastersInLaunch = new HashMap<>();
		this.mastersBeingReturned = new HashMap<>();

		this.sessionState = new SessionState(flinkJar);
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();

		LOG.info("Initializing Mesos dispatcher");

		sessionStore.start();

		// create the scheduler driver to communicate with Mesos
		schedulerCallbackHandler = new SchedulerProxy(self());

		// register with Mesos
		Protos.FrameworkInfo.Builder frameworkInfo = mesosConfig.frameworkInfo()
			.clone()
			.setCheckpoint(true);

		Option<Protos.FrameworkID> frameworkID = sessionStore.getFrameworkID();
		if(frameworkID.isEmpty()) {
			LOG.info("Registering as new framework.");
		}
		else {
			LOG.info("Recovery scenario: re-registering using framework ID {}.", frameworkID.get().getValue());
			frameworkInfo.setId(frameworkID.get());
		}

		MesosConfiguration initializedMesosConfig = mesosConfig.withFrameworkInfo(frameworkInfo);
		MesosConfiguration.logMesosConfig(LOG, initializedMesosConfig);
		schedulerDriver = initializedMesosConfig.createDriver(schedulerCallbackHandler, false);

		// create supporting actors
		connectionMonitor = createConnectionMonitor();
		launchCoordinator = createLaunchCoordinator();
		reconciliationCoordinator = createReconciliationCoordinator();
		taskRouter = createTaskRouter();
	}

	protected ActorRef createConnectionMonitor() {
		return context().actorOf(
			ConnectionMonitor.createActorProps(ConnectionMonitor.class, config),
			"connectionMonitor");
	}

	protected ActorRef createTaskRouter() {
		return context().actorOf(
			Tasks.createActorProps(Tasks.class, config, schedulerDriver, TaskMonitor.class),
			"tasks");
	}

	protected ActorRef createLaunchCoordinator() {
		return context().actorOf(
			LaunchCoordinator.createActorProps(LaunchCoordinator.class, self(), config, schedulerDriver, createOptimizer()),
			"launchCoordinator");
	}

	protected ActorRef createReconciliationCoordinator() {
		return context().actorOf(
			ReconciliationCoordinator.createActorProps(ReconciliationCoordinator.class, config, schedulerDriver),
			"reconciliationCoordinator");
	}

	@Override
	protected void handleMessage(Object message) throws Exception {

		// --- messages about leadership
		if(message instanceof DispatcherMessages.GrantLeadership) {
			grantLeadership(((DispatcherMessages.GrantLeadership) message).leaderSessionID());
		}
		else if(message instanceof DispatcherMessages.RevokeLeadership) {
			revokeLeadership();
		}

		// --- messages about Mesos connection
		else if (message instanceof Registered) {
			registered((Registered) message);
		} else if (message instanceof ReRegistered) {
			reregistered((ReRegistered) message);
		} else if (message instanceof Disconnected) {
			disconnected((Disconnected) message);
		} else if (message instanceof Error) {
			error(((Error) message).message());

			// --- messages about offers
		} else if (message instanceof ResourceOffers || message instanceof OfferRescinded) {
			launchCoordinator.tell(message, self());
		} else if (message instanceof AcceptOffers) {
			acceptOffers((AcceptOffers) message);

			// --- messages about tasks
		} else if (message instanceof StatusUpdate) {
			taskStatusUpdated((StatusUpdate) message);
		} else if (message instanceof ReconciliationCoordinator.Reconcile) {
			// a reconciliation request from a task
			reconciliationCoordinator.tell(message, self());
		} else if (message instanceof TaskMonitor.TaskTerminated) {
			// a termination message from a task
			TaskMonitor.TaskTerminated msg = (TaskMonitor.TaskTerminated) message;
			taskTerminated(msg.taskID(), msg.status());

		} else  {
			// message handled by the generic resource master code
			super.handleMessage(message);
		}
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		// we do not unregister, but cause a hard fail of this process
		LOG.error("FATAL ERROR IN MESOS DISPATCHER: " + message, error);
		LOG.error("Shutting down process");

		// kill this process, this will make an external supervisor (e.g. Marathon) restart the process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	// --------- LEADERSHIP ------------------

	private void grantLeadership(Option<UUID> leaderSessionID) {

		try {
			// recover from the session store
			recoverSessions();

			// connect to Mesos
			connectionMonitor.tell(new ConnectionMonitor.Start(), self());
			schedulerDriver.start();

		} catch (Exception ex) {
			fatalError("unable to take leadership", ex);
		}
	}

	private void revokeLeadership() {
		// disconnect from Mesos
		schedulerDriver.stop(true);

		// todo stop child actors
	}

	// ------------------------------------------------------------------------
	//  Callbacks from the Mesos Master
	// ------------------------------------------------------------------------

	/**
	 * Called when connected to Mesos as a new framework.
	 */
	private void registered(Registered message) {
		connectionMonitor.tell(message, self());

		try {
			sessionStore.setFrameworkID(Option.apply(message.frameworkId()));
		}
		catch(Exception ex) {
			fatalError("unable to store the assigned framework ID", ex);
			return;
		}

		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Called when reconnected to Mesos following a failover event.
	 */
	private void reregistered(ReRegistered message) {
		connectionMonitor.tell(message, self());
		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Called when disconnected from Mesos.
	 */
	private void disconnected(Disconnected message) {
		connectionMonitor.tell(message, self());
		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Handle a task status change.
	 */
	private void taskStatusUpdated(StatusUpdate message) {
		taskRouter.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		schedulerDriver.acknowledgeStatusUpdate(message.status());
	}

	/**
	 * Called when an error is reported by the scheduler callback.
	 */
	private void error(String message) {
		self().tell(new FatalErrorOccurred("Connection to Mesos failed", new Exception(message)), self());
	}

	// ---------  SESSION MGMT -------------

	private void recoverSessions() throws Exception {
		final List<MesosSessionStore.Session> recovered = sessionStore.recoverSessions();

		mastersInNew.clear();
		mastersInLaunch.clear();
		mastersBeingReturned.clear();

		LOG.info("Retrieved {} JobMasters from previous attempt", recovered.size());

		if(!recovered.isEmpty()) {

			List<Tuple2<TaskRequest,String>> toAssign = new ArrayList<>(recovered.size());
			List<LaunchableTask> toLaunch = new ArrayList<>(recovered.size());

			for (final MesosSessionStore.Session master : recovered) {

				switch(master.state()) {
					case New:
						LaunchableMesosSession newSession = sessionState.createLaunchableMesosSession(master);
						mastersInNew.put(extractResourceID(master.taskID()), master);
						toLaunch.add(newSession);
						break;
					case Launched:
						LaunchableMesosSession launchedSession = sessionState.createLaunchableMesosSession(master);
						mastersInLaunch.put(extractResourceID(master.taskID()), master);
						toAssign.add(new Tuple2<>(launchedSession.taskRequest(), master.hostname().get()));
						break;
					case Released:
						mastersBeingReturned.put(extractResourceID(master.taskID()), master);
						break;
				}
				taskRouter.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(master)), self());
			}

			// tell the launch coordinator about prior assignments
			if(toAssign.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Assign(toAssign), self());
			}
			// tell the launch coordinator to launch any new tasks
			if(toLaunch.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Launch(toLaunch), self());
			}
		}
	}

	/**
	 * Accept offers as advised by the launch coordinator.
	 *
	 * Acceptance is routed through this dispatcher to update the persistent state before
	 * forwarding the message to Mesos.
	 */
	private void acceptOffers(AcceptOffers msg) {
		try {
			List<TaskMonitor.TaskGoalStateUpdated> toMonitor = new ArrayList<>(msg.operations().size());

			// transition the persistent state of some tasks to Launched
			for (Protos.Offer.Operation op : msg.operations()) {
				if (op.getType() != Protos.Offer.Operation.Type.LAUNCH) {
					continue;
				}
				for (Protos.TaskInfo info : op.getLaunch().getTaskInfosList()) {
					MesosSessionStore.Session master = mastersInNew.remove(extractResourceID(info.getTaskId()));
					assert (master != null);

					master = master.launchTask(info.getSlaveId(), msg.hostname());
					sessionStore.putSession(master);
					mastersInLaunch.put(extractResourceID(master.taskID()), master);

					LOG.info("Launching Mesos task {} on host {}.",
						master.taskID().getValue(), master.hostname().get());

					toMonitor.add(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(master)));
				}
			}

			// tell the task router about the new plans
			for (TaskMonitor.TaskGoalStateUpdated update : toMonitor) {
				taskRouter.tell(update, self());
			}

			// send the acceptance message to Mesos
			schedulerDriver.acceptOffers(msg.offerIds(), msg.operations(), msg.filters());
		}
		catch(Exception ex) {
			fatalError("unable to accept offers", ex);
		}
	}

	/**
	 * Plan for a new session.
	 * @param sessionParameters the session parameters.
     */
	private void requestSession(SessionParameters sessionParameters) {

	}

	/**
	 * Plan to release a given session.
	 * @param sessionID the Session identifier.
     */
	private void releaseSession(SessionID sessionID) {

	}

	private void taskTerminated(Protos.TaskID taskID, Protos.TaskStatus status) {

	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Extracts a unique ResourceID from the Mesos task.
	 *
	 * @param taskId the Mesos TaskID
	 * @return The ResourceID for the container
	 */
	static ResourceID extractResourceID(Protos.TaskID taskId) {
		return new ResourceID(taskId.getValue());
	}

	/**
	 * Extracts the Mesos task goal state from the session information.
	 * @param session the persistent worker information.
	 * @return goal state information for the {@Link TaskMonitor}.
	 */
	static TaskMonitor.TaskGoalState extractGoalState(MesosSessionStore.Session session) {
		switch(session.state()) {
			case New: return new TaskMonitor.New(session.taskID());
			case Launched: return new TaskMonitor.Launched(session.taskID(), session.slaveID().get());
			case Released: return new TaskMonitor.Released(session.taskID(), session.slaveID().get());
			default: throw new IllegalArgumentException();
		}
	}

	/**
	 * Creates the Fenzo optimizer (builder).
	 * The builder is an indirection to faciliate unit testing of the Launch Coordinator.
	 */
	private static TaskSchedulerBuilder createOptimizer() {
		return new TaskSchedulerBuilder() {
			TaskScheduler.Builder builder = new TaskScheduler.Builder();

			@Override
			public TaskSchedulerBuilder withLeaseRejectAction(Action1<VirtualMachineLease> action) {
				builder.withLeaseRejectAction(action);
				return this;
			}

			@Override
			public TaskScheduler build() {
				return builder.build();
			}
		};
	}


	static class MesosSession implements SessionIDRetrievable, Serializable {
		private SessionID sessionId;
		MesosSession(SessionID sessionId) {
			this.sessionId = sessionId;
		}
		@Override
		public SessionID getSessionID() {
			return null;
		}
	}


	/**
	 * Creates the props needed to instantiate this actor.
	 *
	 * Rather than extracting and validating parameters in the constructor, this factory method takes
	 * care of that. That way, errors occur synchronously, and are not swallowed simply in a
	 * failed asynchronous attempt to start the actor.

	 * @param actorClass
	 *             The actor class, to allow overriding this actor with subclasses for testing.
	 * @param flinkConfig
	 *             The Flink configuration object.
	 * @param mesosConfig
	 *             The Mesos scheduler configuration.
	 * @param jmTaskInfoTemplate
	 *             The template for Mesos tasks launched by the dispatcher.
	 * @param sessionStore
	 *             The session store.
	 * @param leaderElectionService
	 *             The leader election service.
	 * @param log
	 *             The logger to log to.
	 *
	 * @return The Props object to instantiate the MesosDispatcherBackend actor.
	 */
	public static Props createActorProps(
		Class<? extends MesosDispatcherBackend> actorClass,
		Configuration flinkConfig,
		MesosConfiguration mesosConfig,
		Protos.TaskInfo.Builder jmTaskInfoTemplate,
		MesosSessionStore sessionStore,
		SessionDefaults sessionDefaults,
		LeaderElectionService leaderElectionService,
		MesosArtifactServer artifactServer,
		Path flinkJar,
		Logger log) {

		return Props.create(actorClass,
			flinkConfig,
			mesosConfig,
			jmTaskInfoTemplate,
			sessionStore,
			sessionDefaults,
			leaderElectionService,
			artifactServer,
			flinkJar);
	}

	/**
	 * In-memory session state, such as masters in 'new', 'launched', and 'released'.
	 *
	 * Mutates the artifact server state in tandem with the various hashmaps.
	 */
	class SessionState implements MesosArtifactResolver {

//		final Map<ResourceID, MesosSessionStore.Session> mastersInNew;
//		final Map<ResourceID, MesosSessionStore.Session> mastersInLaunch;
//		final Map<ResourceID, MesosSessionStore.Session> mastersBeingReturned;

		private Path flinkJar;

		private SessionState(Path flinkJar) {
			this.flinkJar = flinkJar;
		}

		private LaunchableMesosSession createLaunchableMesosSession(MesosSessionStore.Session session) {

			SessionParameters params = session.params();
			Path sessionRemotePath = new Path(params.jobID().toString());

			// register the artifacts needed by the task with the artifact server.
			// flink.jar
			try {
				artifactServer.addPath(flinkJar, new Path(sessionRemotePath, "flink.jar"));
			} catch (IOException e) {
				throw new RuntimeException("failed to add flink jar to artifact server", e);
			}

			// flink-conf.yaml
			URL confURL;
			try {
				File tempFile = File.createTempFile("flink-conf-", ".yaml");
				tempFile.deleteOnExit();
				BootstrapTools.writeConfiguration(session.params().configuration(), tempFile);
				LOG.info("Wrote session configuration to: {}", tempFile);

				confURL = artifactServer.addPath(
					new Path(tempFile.toURI()), new Path(sessionRemotePath, "flink-conf.yaml"));

			} catch (IOException e) {
				throw new RuntimeException("failed to add flink-conf.yaml to artifact server", e);
			}

			// user artifacts
			for(SessionParameters.Artifact file : params.artifacts()) {
				try {
					artifactServer.addPath(file.localPath(), new Path(sessionRemotePath, file.remotePath()));
				} catch (Exception e) {
					LOG.error("the artifact couldn't be added to the artifact server", e);
					throw new RuntimeException("unusable artifact: " + file.localPath(), e);
				}
			}

			LaunchableMesosSession launchable =
				new LaunchableMesosSession(this, sessionDefaults, session.params(), appMasterTaskInfo, session.taskID());
			return launchable;
		}

		public Option<URL> resolve(JobID jobID, Path remotePath) {
			Path sessionRemotePath = new Path(jobID.toString());
			return artifactServer.resolve(new Path(sessionRemotePath, remotePath));
		}
	}

}
