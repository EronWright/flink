package org.apache.flink.mesos.dispatcher;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.dispatcher.types.SessionIDRetrievable;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Leadership grants write access to the session store and to the cluster manager (if any).
 */
public abstract class AbstractDispatcherBackend<SessionType extends SessionIDRetrievable> extends UntypedActor {

	protected static final int EXIT_CODE_FATAL_ERROR = -13;

	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	protected final Configuration config;

	protected final LeaderElectionService leaderElectionService;

	public AbstractDispatcherBackend(
		Configuration config,
		LeaderElectionService leaderElectionService) {

		requireNonNull(config);
		requireNonNull(leaderElectionService);

		this.config = config;
		this.leaderElectionService = leaderElectionService;
	}

	/**
	 * This method is called by Akka if a new message has arrived for the actor. It logs the
	 * processing time of the incoming message if the logging level is set to debug.
	 *
	 * Important: This method cannot be overriden. The actor specific message handling logic is
	 * implemented by the method handleMessage.
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	@Override
	public final void onReceive(Object message) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received message {} at {} from {}.", message, getSelf().path(), getSender());

			long start = System.nanoTime();

			handleMessage(message);

			long duration = (System.nanoTime() - start)/ 1000000;

			LOG.debug("Handled message {} in {} ms from {}.", message, duration, getSender());
		} else {
			handleMessage(message);
		}
	}

	/**
	 * This method contains the actor logic which defines how to react to incoming messages.
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	protected void handleMessage(Object message) throws Exception {
		if (message instanceof FatalErrorOccurred) {
			FatalErrorOccurred fatalErrorOccurred = (FatalErrorOccurred) message;
			fatalError(fatalErrorOccurred.message(), fatalErrorOccurred.error());
		}

		// --- unknown messages

		else {
			LOG.error("Discarding unknown message: {}", message);
		}
	}

	@Override
	public void preStart() throws Exception {
		try {
			leaderElectionService.start(new Contender());
		}
		catch(Exception ex) {
			LOG.error("Could not start the dispatcher because the leader election service did not start.", ex);
			throw new RuntimeException("Could not start the leader election service.", ex);
		}
	}

	protected abstract void fatalError(String message, Throwable error);

	class Contender implements LeaderContender {

		@Override
		public void grantLeadership(UUID leaderSessionID) {
			self().tell(new DispatcherMessages.GrantLeadership(Option.apply(leaderSessionID)), ActorRef.noSender());
		}

		@Override
		public void revokeLeadership() {
			self().tell(new DispatcherMessages.RevokeLeadership(), ActorRef.noSender());
		}

		@Override
		public String getAddress() {
			return AkkaUtils.getAkkaURL(context().system(), self());
		}

		@Override
		public void handleError(Exception exception) {
			LOG.error("Received an error from the LeaderElectionService.", exception);

			// terminate Dispatcher in case of an error
			self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}
}
