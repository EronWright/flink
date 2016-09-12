package org.apache.flink.mesos.dispatcher.types;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Creation parameters for a session.
 * <p>
 * A session is defined as an instance of a JobManager (JM).   A given
 * JM instance handles exactly one job ("program").
 */
public class SessionParameters implements Serializable {

	/**
	 * Minimum memory requirements, checked by the Client.
	 */
	private static final int MIN_JM_MEMORY = 768; // the minimum memory should be higher than the min heap cutoff
	private static final int MIN_TM_MEMORY = 768;

	public static final ResourceProfile DEFAULT_JM_PROFILE = new ResourceProfile(1.0, 1024);
	public static final ResourceProfile DEFAULT_TM_PROFILE = new ResourceProfile(1.0, 1024);
	;

	private SessionID sessionID;

	private String username;

	private ResourceProfile jmProfile;

	private ResourceProfile tmProfile;

	private int tmCount;

	private int slots;

	private List<Artifact> artifacts;

	private Configuration configuration;

	public SessionParameters(SessionID sessionID, String username, ResourceProfile jmProfile, ResourceProfile tmProfile, int tmCount, int slots, Configuration configuration, List<Artifact> artifacts) {
		this.sessionID = checkNotNull(sessionID);
		this.username = checkNotNull(username);
		this.jmProfile = checkNotNull(jmProfile);
		this.tmProfile = checkNotNull(tmProfile);
		this.tmCount = checkNotNull(tmCount);
		this.slots = checkNotNull(slots);
		this.configuration = checkNotNull(configuration);
		this.artifacts = checkNotNull(artifacts);
	}

	public SessionID sessionID() {
		return sessionID;
	}

	public Configuration configuration() {
		return configuration;
	}

	public String username() {
		return username;
	}

	public ResourceProfile jmProfile() {
		return jmProfile;
	}

	public ResourceProfile tmProfile() {
		return tmProfile;
	}

	public int tmCount() {
		return tmCount;
	}

	public int slots() {
		return slots;
	}

	/**
	 * The artifacts to be copied to the Mesos task sandbox.
	 *
	 * @return a list of artifacts served by the artifact server.
	 */
	public List<Artifact> artifacts() {
		return artifacts;
	}

	/**
	 * A resource profile for a container.
	 */
	public static class ResourceProfile implements Serializable {
		private double cpus;
		private double mem;

		public ResourceProfile(double cpus, double mem) {
			this.cpus = cpus;
			this.mem = mem;
		}

		/**
		 * CPU (in vcores)
		 */
		public double cpus() {
			return cpus;
		}

		/**
		 * Memory (in MB)
		 */
		public double mem() {
			return mem;
		}
	}

	/**
	 * An artifact (file or directory) to deploy into a container.
	 */
	public static class Artifact implements Serializable {
		private final Path localPath;
		private final String remotePath;
		private final boolean cacheable;

		public Artifact(Path localPath, Path remotePath, boolean cacheable) {
			this.localPath = localPath;
			this.remotePath = remotePath.toString();
			this.cacheable = cacheable;
		}

		public Path localPath() {
			return localPath;
		}

		public String remotePath() {
			return remotePath;
		}

		public boolean cacheable() {
			return cacheable;
		}

		@Override
		public String toString() {
			return "Artifact{" +
				"localPath=" + localPath +
				", remotePath='" + remotePath + '\'' +
				", cacheable=" + cacheable +
				'}';
		}
	}

	/**
	 * Create a builder for session parameters.
	 * @return a new builder.
     */
	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder for session parameters.
	 */
	public static class Builder<BuilderType extends Builder> {

		private SessionID sessionID;

		private String name;

		private String username;

		private ResourceProfile jmProfile = DEFAULT_JM_PROFILE;

		private ResourceProfile tmProfile = new ResourceProfile(1.0, 1024);

		private int tmCount = 1;

		private int slots;

		private List<Artifact> artifacts = new ArrayList<>();

		private Configuration configuration;

		public SessionID getSessionID() {
			return this.sessionID;
		}

		public BuilderType setSessionID(SessionID sessionID) {
			this.sessionID = sessionID;
			return (BuilderType) this;
		}

		public BuilderType setName(String name) {
			this.name = name;
			return (BuilderType) this;
		}

		public BuilderType setUsername(String username) {
			this.username = username;
			return (BuilderType) this;
		}

		public BuilderType setJmProfile(ResourceProfile jmProfile) {
			this.jmProfile = jmProfile;
			return (BuilderType) this;
		}

		public BuilderType setTmProfile(ResourceProfile tmProfile) {
			this.tmProfile = tmProfile;
			return (BuilderType) this;
		}

		public BuilderType setTmCount(int tmCount) {
			this.tmCount = tmCount;
			return (BuilderType) this;
		}

		public BuilderType setSlots(int slots) {
			this.slots = slots;
			return (BuilderType) this;
		}

		public BuilderType addArtifacts(Artifact... artifacts) {
			this.artifacts.addAll(Arrays.asList(artifacts));
			return (BuilderType) this;
		}

		public BuilderType setArtifacts(List<Artifact> artifacts) {
			this.artifacts = artifacts;
			return (BuilderType) this;
		}

		public BuilderType setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return (BuilderType) this;
		}

		public SessionParameters build() {

			checkState(sessionID != null, "The session ID must be set");

			checkState(name != null, "The session name must be set");

			checkState(configuration != null, "The configuration must be set");

			checkState(username != null, "The username must be set");

			checkState(tmCount >= 1, "The TaskManager count must be at least 1.");

			checkState(jmProfile != null, "The JobManager resource profile must be specified.");
			checkState(jmProfile.mem() >= MIN_JM_MEMORY,
				"The JobManager memory (%s MB) is below the minimum required memory amount "
					+ "of %s MB", jmProfile.mem(), MIN_JM_MEMORY);
			checkState(jmProfile.cpus() > 0,
				"The JobManager cpu (%s cores) must be positive", jmProfile.cpus());

			checkState(tmProfile != null, "The TaskManager resource profile must be specified.");
			checkState(tmProfile.mem() >= MIN_TM_MEMORY,
				"The TaskManager memory (%s MB) is below the minimum required memory amount "
					+ "of %s MB", tmProfile.mem(), MIN_TM_MEMORY);
			checkState(tmProfile.cpus() > 0,
				"The TaskManager cpu (%s cores) must be positive", tmProfile.cpus());

			checkState(slots > 0, "Number of TaskManager slots must be positive");

			return new SessionParameters(
				sessionID,
				username,
				jmProfile,
				tmProfile,
				tmCount,
				slots,
				configuration,
				artifacts);
		}
	}
}
