package org.apache.flink.mesos.dispatcher.types;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Session status information.
 */
public class SessionStatus implements Serializable {
	private SessionID sessionID;

	private Configuration clientConfiguration;

	public SessionStatus(SessionID sessionID, Configuration clientConfiguration) {
		this.sessionID = sessionID;
		this.clientConfiguration = clientConfiguration;
	}

	/**
	 * Get the session ID.
	 */
	public SessionID getSessionID() {
		return sessionID;
	}

	/**
	 * Get the effective client configuration.
	 */
	public Configuration getClientConfiguration() {
		return clientConfiguration;
	}

	@Override
	public String toString() {
		return "SessionStatus{" +
			"sessionID=" + sessionID +
			", clientConfiguration=\n" + clientConfiguration.toString().replace(",", ",\n") +
			'}';
	}
}
