package org.apache.flink.mesos.client.deployment;

import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;

import java.io.IOException;

/**
 * Dispatcher client to manage Flink sessions.
 */
public interface DispatcherClient extends AutoCloseable {

	/**
	 * Begin to build a new session.
	 */
	SessionParameters.Builder newSession() throws IOException;

	/**
	 * Start a new session.
	 *
	 * @param sessionParams the session parameters.
	 */
	void startSession(SessionParameters sessionParams);

	/**
	 * Stop an existing session.
	 *
	 * @param sessionID the session identifier.
	 */
	void stopSession(SessionID sessionID);
}
