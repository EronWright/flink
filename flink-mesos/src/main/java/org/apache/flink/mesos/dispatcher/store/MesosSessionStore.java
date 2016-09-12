package org.apache.flink.mesos.dispatcher.store;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.mesos.Protos;
import scala.Option;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A store of Mesos sessions and associated framework information.
 */
public interface MesosSessionStore {

	static final DecimalFormat TASKID_FORMAT = new DecimalFormat("jobmanager-00000");

	void start() throws PersistenceException;

	void stop() throws PersistenceException;

	Option<Protos.FrameworkID> getFrameworkID() throws PersistenceException;

	void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws PersistenceException;

	List<Session> recoverTasks() throws PersistenceException;

	Protos.TaskID newTaskID() throws PersistenceException;

	void putTask(Session... tasks) throws PersistenceException;

	void removeTask(Protos.TaskID... taskID) throws PersistenceException;

	void cleanup() throws PersistenceException;

	/**
	 * A stored session.
	 * <p>
	 * A session is realized as a succession of Mesos tasks (i.e. an initial task, followed
	 * by a subsequent task in case of failure).
	 * <p>
	 * The assigned slaveid/hostname is valid in Launched and Released states.  The hostname is needed
	 * by Fenzo for optimization purposes.
	 */
	class Session implements Serializable {

		private final Protos.TaskID taskID;

		private final TaskState state;

		private final int attempt;

		private final Option<Protos.SlaveID> slaveID;

		private final Option<String> hostname;

		private final SessionParameters params;

		private final Configuration dynamicProperties;

		Session(
			SessionParameters params,
			Configuration dynamicProperties,
			Protos.TaskID taskID,
			Option<Protos.SlaveID> slaveID,
			Option<String> hostname,
			TaskState state,
			int attempt) {

			this.taskID = requireNonNull(taskID, "taskID");
			this.slaveID = requireNonNull(slaveID, "slaveID");
			this.hostname = requireNonNull(hostname, "hostname");
			this.state = requireNonNull(state, "state");
			this.params = requireNonNull(params, "params");
			this.dynamicProperties = requireNonNull(dynamicProperties, "dynamicProperties");
			this.attempt = attempt;
		}

		public SessionParameters params() {
			return params;
		}

		@Deprecated
		public SessionID sessionID() {
			return params.sessionID();
		}

		public Protos.TaskID taskID() {
			return taskID;
		}

		public Configuration dynamicProperties() {
			return dynamicProperties;
		}

		public Option<Protos.SlaveID> slaveID() {
			return slaveID;
		}

		public Option<String> hostname() {
			return hostname;
		}

		public TaskState state() {
			return state;
		}

		public TaskReleaseReason reason() {
			return TaskReleaseReason.Failed;
		}

		public int attempt() {
			return attempt;
		}

		// valid transition methods

		/**
		 * Define a new task with the given params and initial task ID.
		 */
		public static Session newTask(
			SessionParameters params,
			Configuration dynamicProperties,
			Protos.TaskID taskID) {
			return new Session(
				params,
				dynamicProperties,
				taskID,
				Option.<Protos.SlaveID>empty(), Option.<String>empty(),
				TaskState.New,
				1);
		}

		/**
		 * Launch the task onto the given slave with the given dynamic properties.
		 */
		public Session launchTask(
			Configuration dynamicProperties,
			Protos.SlaveID slaveID,
			String hostname) {
			checkState(state == TaskState.New);
			return new Session(params, dynamicProperties,
				taskID, Option.apply(slaveID), Option.apply(hostname), TaskState.Launched,
				attempt);
		}

		/**
		 * Reschedule a released task.
		 * Recover the session with a new task.
		 *
		 * @param taskID the ID of the replacement task.
		 */
		public Session rescheduleTask(Configuration dynamicProperties, Protos.TaskID taskID) {
			checkState(state == TaskState.Released);
			return new Session(params, dynamicProperties,
				taskID, Option.<Protos.SlaveID>empty(), Option.<String>empty(), TaskState.New,
				attempt + 1);
		}

		/**
		 * Release the task.
		 */
		public Session releaseTask(TaskReleaseReason reason) {
			checkState(state == TaskState.Launched);
			return new Session(params, dynamicProperties, taskID, slaveID, hostname,
				TaskState.Released, attempt);
		}
	}

	enum TaskState {
		New, Launched, Released
	}

	enum TaskReleaseReason {
		Failed,
		Stopped
	}

}
