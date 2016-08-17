package org.apache.flink.mesos.dispatcher.store;

import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.mesos.Protos;
import scala.Option;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A store of Mesos sessions and associated framework information.
 */
public interface MesosSessionStore {

	static final DecimalFormat TASKID_FORMAT = new DecimalFormat("session-00000");

	void start() throws Exception;

	void stop() throws Exception;

	Option<Protos.FrameworkID> getFrameworkID() throws Exception;

	void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception;

	List<Session> recoverSessions() throws Exception;

	Protos.TaskID newTaskID() throws Exception;

	void putSession(Session session) throws Exception;

	void removeSession(Protos.TaskID taskID) throws Exception;

	void cleanup() throws Exception;

	/**
	 * A stored session.
	 *
	 * The assigned slaveid/hostname is valid in Launched and Released states.  The hostname is needed
	 * by Fenzo for optimization purposes.
	 */
	static class Session implements Serializable {

		private SessionParameters params;

		private Protos.TaskID taskID;

		private Option<Protos.SlaveID> slaveID;

		private Option<String> hostname;

		private TaskState state;

		public Session(SessionParameters params, Protos.TaskID taskID, Option<Protos.SlaveID> slaveID, Option<String> hostname, TaskState state) {
			requireNonNull(params, "params");
			requireNonNull(taskID, "taskID");
			requireNonNull(slaveID, "slaveID");
			requireNonNull(hostname, "hostname");
			requireNonNull(state, "state");

			this.params = params;
			this.taskID = taskID;
			this.slaveID = slaveID;
			this.hostname = hostname;
			this.state = state;
		}

		public SessionParameters params() {
			return params;
		}

		public Protos.TaskID taskID() {
			return taskID;
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

		// valid transition methods

		public static Session newTask(SessionParameters params, Protos.TaskID taskID) {
			return new Session(
				params,
				taskID,
				Option.<Protos.SlaveID>empty(), Option.<String>empty(),
				TaskState.New);
		}

		public Session launchTask(Protos.SlaveID slaveID, String hostname) {
			return new Session(params, taskID, Option.apply(slaveID), Option.apply(hostname), TaskState.Launched);
		}

		public Session releaseTask() {
			return new Session(params, taskID, slaveID, hostname, TaskState.Released);
		}

		@Override
		public String toString() {
			return "Session{" +
				"taskID=" + taskID +
				", slaveID=" + slaveID +
				", hostname=" + hostname +
				", state=" + state +
				", params=" + params +
				'}';
		}
	}

	enum TaskState {
		New,Launched,Released
	}
}
