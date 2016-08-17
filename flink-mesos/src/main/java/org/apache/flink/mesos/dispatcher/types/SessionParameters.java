package org.apache.flink.mesos.dispatcher.types;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import scala.Option;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

/**
 * Creation parameters for a session.
 *
 * A session is defined as an instance of a JobManager (JM).   A given
 * JM instance handles exactly one job ("program").
 */
public class SessionParameters implements Serializable {

	private String username;

	private ResourceProfile jmProfile;

	private ResourceProfile tmProfile;

	private int tmCount;

	private int slots;

	private List<Artifact> artifacts;

	private JobID jobID;

	private List<ProgramSpec> programSpec;

	private Configuration configuration = new Configuration();

	public SessionParameters(String username, ResourceProfile jmProfile, ResourceProfile tmProfile, int tmCount, int slots, JobID jobID, List<Artifact> artifacts, List<ProgramSpec> programSpec) {
		this.username = username;
		this.jmProfile = jmProfile;
		this.tmProfile = tmProfile;
		this.tmCount = tmCount;
		this.slots = slots;
		this.jobID = jobID;
		this.artifacts = artifacts;
		this.programSpec = programSpec;
	}

	public JobID jobID() {
		return jobID;
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
	 * The list of programs to execute.
     */
	public List<ProgramSpec> programSpec() {
		return programSpec;
	}

	/**
	 * The artifacts to be copied to the Mesos task sandbox.
	 * @return a list of artifacts served by the artifact server.
	 */
	public List<Artifact> artifacts() {
		return artifacts;
	}

	public static class ResourceProfile implements Serializable {
		private double cpus;
		private double mem;

		public ResourceProfile(double cpus, double mem) {
			this.cpus = cpus;
			this.mem = mem;
		}

		public double cpus() {
			return cpus;
		}

		public double mem() {
			return mem;
		}
	}

	public static class Artifact implements Serializable {
		private final Path localPath;
		private final String remotePath;
		private final boolean cacheable;

		public Artifact(Path localPath, String remotePath, boolean cacheable) {
			this.localPath = localPath;
			this.remotePath = remotePath;
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

	public static class ProgramSpec implements Serializable {
//		private final String jarFilePath;
//
//		private final String entryPointClass;
//
//		private final List<URL> classpaths;
//
//		private final String[] programArgs;
//
//		private final int parallelism;
//
//		private final String savepointPath;
	}
}
