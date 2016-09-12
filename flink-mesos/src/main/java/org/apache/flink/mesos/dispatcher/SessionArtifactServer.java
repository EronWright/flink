package org.apache.flink.mesos.dispatcher;

import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * A session-aware artifact server.
 */
public class SessionArtifactServer implements MesosArtifactResolver {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	private final MesosArtifactServer artifactServer;
	private final Path flinkJar;

	public SessionArtifactServer(MesosArtifactServer artifactServer, Path flinkJar) {
		this.artifactServer = artifactServer;
		this.flinkJar = flinkJar;
	}

	/**
	 * Add the given session to the artifact server.
	 */
	public void add(MesosSessionStore.Session session) {
		SessionParameters params = session.params();
		Path sessionRemotePath = new Path(params.sessionID().toString());

		if (artifactServer.resolve(new Path(sessionRemotePath, "flink.jar")).isDefined()) {
			// the session was already added
			return;
		}

		// register the artifacts needed by the task with the artifact server.
		// flink.jar
		try {
			artifactServer.addPath(flinkJar, new Path(sessionRemotePath, "flink.jar"));
		} catch (IOException e) {
			throw new RuntimeException("failed to add flink jar to artifact server", e);
		}

		// flink-conf.yaml
		try {
			File tempFile = File.createTempFile("flink-conf-", ".yaml");
			tempFile.deleteOnExit();
			BootstrapTools.writeConfiguration(session.params().configuration(), tempFile);
			LOG.info("Wrote session configuration to: {}", tempFile);

			artifactServer.addPath(
				new Path(tempFile.toURI()), new Path(sessionRemotePath, "flink-conf.yaml"));

		} catch (IOException e) {
			throw new RuntimeException("failed to add flink-conf.yaml to artifact server", e);
		}

		// user artifacts
		for (SessionParameters.Artifact file : params.artifacts()) {
			try {
				artifactServer.addPath(file.localPath(), new Path(sessionRemotePath, file.remotePath()));
			} catch (Exception e) {
				LOG.error("the artifact couldn't be added to the artifact server", e);
				throw new RuntimeException("unusable artifact: " + file.localPath(), e);
			}
		}
	}

	/**
	 * Remove the given session from the artifact server.
	 */
	public void remove(MesosSessionStore.Session session) {
		SessionParameters params = session.params();
		Path sessionRemotePath = new Path(params.sessionID().toString());
		artifactServer.removePath(new Path(sessionRemotePath, "flink.jar"));
		artifactServer.removePath(new Path(sessionRemotePath, "flink-conf.yaml"));
		for (SessionParameters.Artifact file : params.artifacts()) {
			artifactServer.removePath(new Path(sessionRemotePath, file.remotePath()));
		}
	}

	/**
	 * Resolve the given session-specific path.
	 *
	 * @param sessionID  the session.
	 * @param remotePath the remote path of the artifact in the given session.
	 * @return a fully-qualified URL for use by the Mesos fetcher.
	 */
	public Option<URL> resolve(SessionID sessionID, Path remotePath) {
		Path sessionRemotePath = new Path(sessionID.toString());
		return artifactServer.resolve(new Path(sessionRemotePath, remotePath));
	}
}
