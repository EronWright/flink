package org.apache.flink.mesos.dispatcher.store;

import java.io.IOException;

/**
 * Occurs when the store cannot access the underlying persistence mechanism.
 */
public class PersistenceException extends IOException {
	private static final long serialVersionUID = 1L;

	public PersistenceException(String message) {
		super(message);
	}

	public PersistenceException(String message, Exception innerException) {
		super(message, innerException);
	}
}
