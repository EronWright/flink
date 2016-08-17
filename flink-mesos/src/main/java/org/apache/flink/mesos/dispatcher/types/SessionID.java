package org.apache.flink.mesos.dispatcher.types;

import java.io.Serializable;
import java.util.Objects;

/**
 */
public class SessionID implements Serializable {

	private static final long serialVersionUID = 42L;

	private final String sessionId;

	public SessionID(String sessionId) {
		this.sessionId = sessionId;
	}

	public String sessionId() {
		return sessionId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SessionID sessionID = (SessionID) o;
		return Objects.equals(sessionId, sessionID.sessionId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sessionId);
	}

	@Override
	public String toString() {
		return "SessionID{" +
			"sessionId='" + sessionId + '\'' +
			'}';
	}
}
