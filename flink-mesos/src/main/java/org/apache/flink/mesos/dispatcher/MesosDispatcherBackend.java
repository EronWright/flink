package org.apache.flink.mesos.dispatcher;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.store.PersistenceException;
import org.apache.flink.mesos.dispatcher.types.SessionDefaults;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionIDRetrievable;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.dispatcher.types.SessionStatus;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.SchedulerProxy;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.Tasks;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import scala.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 * Dispatcher backend for Apache Mesos.
 *
 * Uses Mesos tasks to launch jobmaster instances to support Flink sessions.
 *
 * Leadership bestows write access to Mesos.
 */
public class MesosDispatcherBackend
	extends AbstractDispatcherBackend<MesosDispatcherBackend.MesosSession> {

	private static final int MAX_ATTEMPTS = 3;

	/** The Mesos configuration (master and framework info) */
	private final MesosConfiguration mesosConfig;

	/** Context information used to start a AppMaster Java process */
	private final Protos.TaskInfo.Builder appMasterTaskInfo;

	/** The persistent session store */
	private final MesosSessionStore sessionStore;

	private final TaskCache taskCache;

	/** Callback handler for the asynchronous Mesos scheduler */
	private SchedulerProxy schedulerCallbackHandler;

	/** Mesos scheduler driver */
	private SchedulerDriver schedulerDriver;

	private ActorRef connectionMonitor;

	private ActorRef taskRouter;

	private ActorRef launchCoordinator;

	private ActorRef reconciliationCoordinator;

	private SessionArtifactServer artifactServer;

	private SessionDefaults sessionDefaults;

	private ActorRef sessionStatusListener;

	public MesosDispatcherBackend(
		Configuration flinkConfig,
		MesosConfiguration mesosConfig,
		Protos.TaskInfo.Builder appMasterTaskInfo,
		MesosSessionStore sessionStore,
		SessionDefaults sessionDefaults,
		LeaderElectionService leaderElectionService,
		SessionArtifactServer artifactServer,
		ActorRef sessionStatusListener) {

		super(flinkConfig, leaderElectionService);

		this.mesosConfig = requireNonNull(mesosConfig);
		this.appMasterTaskInfo = requireNonNull(appMasterTaskInfo);
		this.sessionStore = requireNonNull(sessionStore);
		this.sessionDefaults = requireNonNull(sessionDefaults);
		this.artifactServer = requireNonNull(artifactServer);
		this.sessionStatusListener = sessionStatusListener;

		this.taskCache = new TaskCache(sessionStore);
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

		// --- messages about sessions
		else if (message instanceof DispatcherMessages.StartSession) {
			requestSession(((DispatcherMessages.StartSession) message).params());
		}
		else if (message instanceof DispatcherMessages.StopSession) {
			releaseSession(((DispatcherMessages.StopSession) message).sessionID());
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
		} else if (message instanceof TaskMonitor.TaskStarted) {
			// a started message from a task
			TaskMonitor.TaskStarted msg = (TaskMonitor.TaskStarted) message;
			taskStarted(msg.taskID(), msg.status());

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

	// ------------------------------------------------------------------------
	// Dispatcher leadership
	// ------------------------------------------------------------------------

	private void grantLeadership(Option<UUID> leaderSessionID) {

		try {
			// recover from persistent store
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
	 * Called when an error is reported by the scheduler callback.
	 */
	private void error(String message) {
		self().tell(new FatalErrorOccurred("Connection to Mesos failed", new Exception(message)), self());
	}

	// ------------------------------------------------------------------------
	//  Session management
	// ------------------------------------------------------------------------

	private void recoverSessions() throws Exception {
		
		// recover the tasks that host the sessions
		final List<MesosSessionStore.Session> recovered = taskCache.recoverTasks();
		LOG.info("Retrieved {} session tasks from previous attempt", recovered.size());

		List<Tuple2<TaskRequest,String>> toAssign = new ArrayList<>(recovered.size());
		List<LaunchableTask> toLaunch = new ArrayList<>(recovered.size());

		// scan the tasks
		Map<SessionID, MesosSessionStore.Session> latestTaskForSession = new HashMap<>();
		LaunchableMesosSession launchable;
		for (final MesosSessionStore.Session task : recovered) {
			switch(task.state()) {
				case New:
					artifactServer.add(task);
					launchable = createLaunchableMesosSession(task);
					toLaunch.add(launchable);
					break;
				case Launched:
					artifactServer.add(task);
					launchable = createLaunchableMesosSession(task);
					toAssign.add(new Tuple2<>(launchable.taskRequest(), task.hostname().get()));
					break;
				case Released:
					break;
			}

			// tell the task router about the new plans
			taskRouter.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(task)), self());
		}

//		// start replacement tasks
//		for(Map.Entry<SessionID,List<MesosSessionStore.Session>> session : taskCache.getSessions().entrySet()) {
//			assert(session.getValue().size() >= 1);
//			MesosSessionStore.Session latestTask = session.getValue().get(session.getValue().size() - 1);
//			if(latestTask.state() == MesosSessionStore.TaskState.Released &&
//				latestTask.reason() == MesosSessionStore.TaskReleaseReason.Failed) {
//				rescheduleTask(latestTask);
//			}
//		}

		// tell the launch coordinator about prior assignments
		if(toAssign.size() >= 1) {
			launchCoordinator.tell(new LaunchCoordinator.Assign(toAssign), self());
		}
		// tell the launch coordinator to launch any new tasks
		if(toLaunch.size() >= 1) {
			launchCoordinator.tell(new LaunchCoordinator.Launch(toLaunch), self());
		}
	}

	/**
	 * Plan for a new session.
	 * @param sessionParameters the session parameters.
	 */
	private void requestSession(SessionParameters sessionParameters) {
		try {
			if(!taskCache.findTasks(sessionParameters.sessionID()).isEmpty()) {
				// session already exists
				// todo produce an error
				return;
			}

			// generate a session task into persistent state
			MesosSessionStore.Session task = MesosSessionStore.Session.newTask(
				sessionParameters, new Configuration(), sessionStore.newTaskID());
			taskCache.putTask(task);

			// schedule the task using the launch coordinator
			scheduleTask(task);
		}
		catch(Exception ex) {
			fatalError("unable to request a new session", ex);
		}
	}

	/**
	 * Schedule the given task.
	 */
	private void scheduleTask(MesosSessionStore.Session task) {
		checkArgument(task.state() == MesosSessionStore.TaskState.New);

		LOG.info("Scheduling Mesos task {} with ({} MB, {} cpus).",
			task.taskID(), task.params().tmProfile().mem(), task.params().tmProfile().cpus());

		LaunchableMesosSession launchable = createLaunchableMesosSession(task);

		artifactServer.add(task);

		// tell the task router about the new plans
		taskRouter.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(task)), self());

		// tell the launch coordinator to launch the new task
		launchCoordinator.tell(new LaunchCoordinator.Launch(
			Collections.singletonList((LaunchableTask) launchable)), self());
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

			// transition the persistent state of some sessions to Launched
			for (Protos.Offer.Operation op : msg.operations()) {
				if (op.getType() != Protos.Offer.Operation.Type.LAUNCH) {
					continue;
				}
				for (Protos.TaskInfo info : op.getLaunch().getTaskInfosList()) {
					Option<MesosSessionStore.Session> task = taskCache.getTask(info.getTaskId());
					if(task.isEmpty()) {
						LOG.error("Attempted to accept an offer for non-existent task {}", info.getTaskId());
						throw new IllegalStateException("task unexpectedly missing");
					}

					Option<LaunchableTask> launchable = msg.findLaunchableTask(info.getTaskId());
					if(launchable.isEmpty()) {
						throw new IllegalStateException("invalid AcceptOffers messsage");
					}
					LaunchableMesosSession launchableTask = (LaunchableMesosSession) launchable.get();

					MesosSessionStore.Session launched = task.get().launchTask(
						launchableTask.dynamicProperties(), info.getSlaveId(), msg.hostname());
					taskCache.putTask(launched);

					LOG.info("Launching Mesos task {} on host {}.",
						launched.taskID().getValue(), launched.hostname().get());

					toMonitor.add(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(launched)));
				}
			}

			// send the acceptance message to Mesos
			schedulerDriver.acceptOffers(msg.offerIds(), msg.operations(), msg.filters());

			// tell the task router about the new plans
			for (TaskMonitor.TaskGoalStateUpdated update : toMonitor) {
				taskRouter.tell(update, self());
			}
		}
		catch(Exception ex) {
			fatalError("unable to accept offers", ex);
		}
	}

	/**
	 * Plan to release a given session.
	 * @param sessionID the Session identifier.
     */
	private void releaseSession(SessionID sessionID) {
		try {
			LOG.info("Releasing session {}", sessionID);
			List<MesosSessionStore.Session> tasks = taskCache.findTasks(sessionID);

			for(MesosSessionStore.Session task : tasks) {
				if(task.state() == MesosSessionStore.TaskState.Released) {
					continue;
				}

				LOG.info("Releasing task {} of session {}", task.taskID(), sessionID);
				MesosSessionStore.Session released =
					task.releaseTask(MesosSessionStore.TaskReleaseReason.Stopped);
				taskCache.putTask(released);

				// tell the task router about the updated plans
				taskRouter.tell(
					new TaskMonitor.TaskGoalStateUpdated(extractGoalState(released)), self());

				if (released.hostname().isDefined()) {
					// tell the launch coordinator that the task is being unassigned
					// from the host, for planning purposes
					launchCoordinator.tell(new LaunchCoordinator.Unassign(
						released.taskID(), released.hostname().get()), self());
				}
			}

		} catch (Exception ex) {
			fatalError("unable to release session", ex);
		}
	}

	// ------------------------------------------------------------------------
	//  Callbacks related to tasks
	// ------------------------------------------------------------------------

	/**
	 * Handle a task status change.
	 */
	private void taskStatusUpdated(StatusUpdate message) {
		taskCache.putTaskStatus(message.status());
		taskRouter.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		schedulerDriver.acknowledgeStatusUpdate(message.status());
	}

	/**
	 * Handle a started task.
	 * @param taskID
	 * @param taskStatus
	 */
	private void taskStarted(Protos.TaskID taskID, Protos.TaskStatus taskStatus) {

		Option<MesosSessionStore.Session> task = taskCache.getTask(taskID);
		if(!task.isDefined()) {
			LOG.warn("Received a TaskStarted message for unknown task {}", taskID);
			return;
		}

		SessionID sessionID = task.get().params().sessionID();

		// double-check that the task corresponds to the latest attempt
		List<MesosSessionStore.Session> tasksInSession = taskCache.findTasks(sessionID);
		assert(tasksInSession.size() >= 1);
		if(!taskID.equals(tasksInSession.get(tasksInSession.size() - 1).taskID())) {
			LOG.warn("Received a TaskStarted message for obsolete task {}", taskID);
			return;
		}

		if(sessionStatusListener != null) {
			Configuration clientConfig = LaunchableMesosSession.getClientConfiguration(
				task.get().params(), task.get().dynamicProperties(), Option.apply(taskStatus));

			SessionStatus status = new SessionStatus(sessionID, clientConfig);

			LOG.debug("Sending SessionStatusUpdate to listener(s): {}", status);
			sessionStatusListener.tell(new DispatcherMessages.SessionStatusUpdate(status), self());
		}
	}

	/**
	 * Handle a task termination notice (as provided by the task monitor).
     */
	private void taskTerminated(Protos.TaskID taskID, Protos.TaskStatus status) {

		// this callback occurs for failed tasks and for released tasks alike

		final Option<MesosSessionStore.Session> terminated = taskCache.getTask(taskID);
		if(terminated.isEmpty()) {
			LOG.info("Received a termination notice for obsolete task {}", taskID);
			return;
		}
		SessionID sessionID = terminated.get().params().sessionID();
//		List<MesosSessionStore.Session> sessionTasks = taskCache.findTasks(sessionID);

		if(terminated.get().state() == MesosSessionStore.TaskState.Released) {
			// a planned termination
			LOG.info("Task {} for session {} finished successfully with diagnostics: {}",
				taskID, sessionID, status.getMessage());
			artifactServer.remove(terminated.get());
		}
		else {
			// unplanned termination (task failed unexpectedly)
			LOG.warn("Task {} failed for session {}.  State: {} Reason: {} ({})",
				taskID.getValue(), sessionID,
				status.getState(), status.getReason(), status.getMessage());

			// release the task and (optionally) plan for a replacement
			try {
				MesosSessionStore.Session released =
					terminated.get().releaseTask(MesosSessionStore.TaskReleaseReason.Failed);

				// create a replacement task
				MesosSessionStore.Session rescheduled = null;
				if(released.attempt() >= MAX_ATTEMPTS) {
					LOG.warn("Session {} exceeded {} attempts, not rescheduling.",
						sessionID, MAX_ATTEMPTS);
					taskCache.putTask(released);
				}
				else {
					rescheduled =
						released.rescheduleTask(new Configuration(), sessionStore.newTaskID());
					LOG.info("Scheduling replacement task {} for session {}.",
						rescheduled.taskID().getValue(), sessionID);

					// transactional put
					taskCache.putTask(released, rescheduled);
				}

				if (released.hostname().isDefined()) {
					// tell the launch coordinator that the task is being unassigned
					// from the host, for planning purposes
					launchCoordinator.tell(new LaunchCoordinator.Unassign(
						released.taskID(), released.hostname().get()), self());
				}

				if(rescheduled != null) {
					scheduleTask(rescheduled);
				}

			} catch (Exception e) {
				LOG.error("Unable to create a replacement task for session {}", sessionID);
				fatalError("Unable to access session store", e);
				return;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private LaunchableMesosSession createLaunchableMesosSession(MesosSessionStore.Session session) {

		LaunchableMesosSession launchable =
			new LaunchableMesosSession(
				artifactServer, sessionDefaults, session.params(),
				appMasterTaskInfo, session.taskID());
		return launchable;
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
	 * @param leaderElectionService
	 *             The leader election service.
	 * @param sessionStore
	 *             The session store.
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
		SessionArtifactServer artifactServer,
		ActorRef sessionStatusListener,
		Logger log) {

		return Props.create(actorClass,
			flinkConfig,
			mesosConfig,
			jmTaskInfoTemplate,
			sessionStore,
			sessionDefaults,
			leaderElectionService,
			artifactServer,
			sessionStatusListener);
	}

	/**
	 * A representation of a session as provided by the Mesos dispatcher.
	 */
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
	 * An indexed cache of the persisted task information.
	 */
	static class TaskCache implements Comparator<MesosSessionStore.Session> {

		private final MesosSessionStore sessionStore;

		final Map<Protos.TaskID, MesosSessionStore.Session> tasks = new HashMap<>();
		final Map<Protos.TaskID, Protos.TaskStatus> taskStatuses = new HashMap<>();

		/**
		 * A reverse index of tasks by their associated session IDs.
		 * Numerous tasks may exist for a single session - for example,
		 * the backend may spawn a task to restart a session, while
		 * the prior task is still being cleaned up.
		 */
		final Map<SessionID, List<MesosSessionStore.Session>> sessionIndex = new HashMap<>();

		public TaskCache(MesosSessionStore sessionStore) {
			this.sessionStore = sessionStore;
		}

		public List<MesosSessionStore.Session> recoverTasks() throws PersistenceException {

			tasks.clear();
			sessionIndex.clear();
			final List<MesosSessionStore.Session> recovered = sessionStore.recoverTasks();

			Set<Protos.TaskID> obsoleteStatuses = new HashSet<>(taskStatuses.keySet());

			for (final MesosSessionStore.Session task : recovered) {
				tasks.put(task.taskID(), task);

				List<MesosSessionStore.Session> tasksForSession;
				if(sessionIndex.containsKey(task.params().sessionID())) {
					tasksForSession = sessionIndex.get(task.params().sessionID());
				}
				else {
					tasksForSession = new ArrayList<MesosSessionStore.Session>(1);
					sessionIndex.put(task.params().sessionID(), tasksForSession);
				}
				tasksForSession.add(task);
				Collections.sort(tasksForSession, this);

				obsoleteStatuses.remove(task.taskID());
			}

			for(Protos.TaskID taskIDs : obsoleteStatuses) {
				taskStatuses.remove(taskIDs);
			}

			return recovered;
		}

		public Map<SessionID, List<MesosSessionStore.Session>> getSessions() {
			return Collections.unmodifiableMap(sessionIndex);
		}

		/**
		 * Get the task with the corresponding task ID.
         */
		public Option<MesosSessionStore.Session> getTask(Protos.TaskID taskID) {
			if(tasks.containsKey(taskID)) {
				return Option.apply(tasks.get(taskID));
			}
			else {
				return Option.empty();
			}
		}

		/**
		 * Find tasks associated with the given session ID.
		 * In an HA configuration, more than one task may be associated with a single session.
		 *
         * @return a list of tasks, or empty if the dispatcher has no record of that session.
         */
		List<MesosSessionStore.Session> findTasks(SessionID sessionID) {
			if(!sessionIndex.containsKey(sessionID)) {
				return Collections.emptyList();
			}
			return sessionIndex.get(sessionID);
		}

		/**
		 * Put the given task into persistent storage, and update the cache.
		 * @param tasksToPut the task(s).
         */
		void putTask(MesosSessionStore.Session... tasksToPut) throws PersistenceException {
			sessionStore.putTask(tasksToPut);

			for(MesosSessionStore.Session task : tasksToPut) {
				MesosSessionStore.Session prior = tasks.put(task.taskID(), task);
				List<MesosSessionStore.Session> tasksForSession;
				if (sessionIndex.containsKey(task.params().sessionID())) {
					tasksForSession = sessionIndex.get(task.params().sessionID());
				} else {
					tasksForSession = new ArrayList<MesosSessionStore.Session>(1);
					sessionIndex.put(task.params().sessionID(), tasksForSession);
				}

				if (!tasksForSession.contains(task.taskID())) {
					tasksForSession.add(task);
					Collections.sort(tasksForSession, this);
				}
			}
		}

		/**
		 * Remove the task with the given task ID.
         */
		void removeTask(Protos.TaskID taskID) throws PersistenceException {
			sessionStore.removeTask(taskID);
			MesosSessionStore.Session prior = tasks.remove(taskID);
			if(prior != null) {
				if(sessionIndex.containsKey(prior.params().sessionID())) {
					List<MesosSessionStore.Session> tasksForSession =
						sessionIndex.get(prior.params().sessionID());
					tasksForSession.remove(taskID);
					if(tasksForSession.isEmpty()) {
						sessionIndex.remove(prior.params().sessionID());
					}
				}
				taskStatuses.remove(prior.taskID());
			}
		}

		public Option<Protos.TaskStatus> getTaskStatus(Protos.TaskID taskID) {
			if(!taskStatuses.containsKey(taskID)) {
				return Option.empty();
			}
			else {
				return Option.apply(taskStatuses.get(taskID));
			}
		}

		public void putTaskStatus(Protos.TaskStatus taskStatus) {
			taskStatuses.put(taskStatus.getTaskId(), taskStatus);
		}

		@Override
		public int compare(MesosSessionStore.Session o1, MesosSessionStore.Session o2) {
			return o1.attempt() - o2.attempt();
		}
	}

}
