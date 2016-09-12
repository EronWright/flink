package org.apache.flink.mesos.dispatcher.types;

/**
 * Implemented by classes that have an associated session ID.
 */
public interface SessionIDRetrievable {
	SessionID getSessionID();
}
