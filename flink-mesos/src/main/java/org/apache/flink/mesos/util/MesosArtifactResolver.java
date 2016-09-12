package org.apache.flink.mesos.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import scala.Option;

import java.net.URL;

/**
 * Resolves session artifacts.
 */
public interface MesosArtifactResolver {
	/**
	 * Resolve the remote path as a fetchable Mesos artifact.
	 * @param remotePath the remote path of the artifact in the given session.
	 * @return a fully-qualified artifact.
     */
	Option<URL> resolve(SessionID session, Path remotePath);
}
