package org.apache.flink.mesos.client.deployment;

import org.apache.flink.mesos.dispatcher.types.SessionStatus;

/**
 * Listener for dispatcher client events.
 */
public interface DispatcherClientListener {

	/**
	 * This methos is called when a session status change occurs.
	 *
	 * @param client the dispatcher client providing the callback.
	 * @param status the updated session status.
	 */
	void statusUpdate(DispatcherClient client, SessionStatus status);

}
