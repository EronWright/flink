package org.apache.flink.mesos.client.deployment;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.DispatcherMessages;
import org.apache.flink.mesos.dispatcher.DispatcherUtils;
import org.apache.flink.mesos.dispatcher.MesosDispatcherBackend;
import org.apache.flink.mesos.dispatcher.SessionArtifactServer;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.types.SessionDefaults;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Client for a local dispatcher.
 */
public class LocalDispatcherClient implements DispatcherClient {
	private static final Logger LOG = LoggerFactory.getLogger(LocalDispatcherClient.class);

	private final DispatcherClientListener listener;

	private final ActorSystem actorSystem;

	private final Configuration config;
	private final Path flinkJar;

	private MesosArtifactServer artifactServer;
	private SessionArtifactServer sessionArtifactServer;

	private ActorRef dispatcher;

	/**
	 * Create a new local dispatcher client.
	 *
	 * @param listener the listener for event callbacks.
	 * @param config   the Flink configuration to use for the local dispatcher.
	 * @param flinkJar the location of the Flink jar.
	 */
	public LocalDispatcherClient(
		DispatcherClientListener listener, Configuration config, Path flinkJar) {
		this.listener = checkNotNull(listener);
		this.config = checkNotNull(config);
		this.flinkJar = checkNotNull(flinkJar);

		this.actorSystem = AkkaUtils.createLocalActorSystem(config);

		try {
			createLocalDispatcher();
		} catch (Throwable t) {
			LOG.error("unable to create a local dispatcher");
			try {
				this.close();
			} catch (Exception e) {
				LOG.warn("unable to cleanup", e);
			}
			throw new IllegalStateException("unable to create a local dispatcher");
		}
	}

	/**
	 * Close the dispatcher.
	 */
	@Override
	public void close() throws Exception {
		try {
			actorSystem.shutdown();
		} catch (Throwable t) {
			LOG.warn("failed to shutdown the actorsystem", t);
		}
	}

	/**
	 * Create the local dispatcher actor and related compoents.
	 */
	private void createLocalDispatcher() throws Exception {
		LeaderElectionService leaderElectionService = new StandaloneLeaderElectionService();

		final String dispatcherHostname = InetAddress.getLocalHost().getHostName();
		final MesosConfiguration mesosConfig = DispatcherUtils.createMesosConfig(config, dispatcherHostname);

		String prefix = UUID.randomUUID().toString();
		artifactServer = new MesosArtifactServer(prefix, dispatcherHostname, 0);
		sessionArtifactServer = new SessionArtifactServer(artifactServer, flinkJar);

		// session-related
		MesosSessionStore sessionStore = DispatcherUtils.createSessionStore(config);
		SessionDefaults sessionDefaults = createSessionDefaults(config);
		Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder();

		// listener
		Props listenerProps = ListenerActor.createActorProps(this);
		ActorRef listenerRef = actorSystem.actorOf(listenerProps);

		Props backendProps = MesosDispatcherBackend.createActorProps(
			MesosDispatcherBackend.class, config,
			mesosConfig, taskInfo, sessionStore, sessionDefaults,
			leaderElectionService, sessionArtifactServer, listenerRef, LOG);

		dispatcher = actorSystem.actorOf(backendProps);
	}

	private static SessionDefaults createSessionDefaults(Configuration flinkConfig) {
		String dispatcherNamespace = flinkConfig.getString(
			ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY,
			ConfigConstants.DEFAULT_ZOOKEEPER_NAMESPACE_KEY);

		SessionDefaults defaults = new SessionDefaults(
			ZKPaths.makePath(dispatcherNamespace, "sessions"));

		return defaults;
	}

	/**
	 * Creates a session builder for a new session.
	 *
	 * @return a new builder
	 * @throws IOException if a remote communication fails
	 */
	@Override
	public SessionParameters.Builder newSession() throws IOException {
		SessionParameters.Builder params = SessionParameters
			.newBuilder()
			.setSessionID(SessionID.generate());
		return params;
	}

	@Override
	public void startSession(SessionParameters sessionParams) {
		dispatcher.tell(new DispatcherMessages.StartSession(sessionParams), ActorRef.noSender());
	}

	@Override
	public void stopSession(SessionID sessionID) {
		dispatcher.tell(new DispatcherMessages.StopSession(sessionID), ActorRef.noSender());
	}

	@VisibleForTesting
	void handleSessionStatusUpdate(DispatcherMessages.SessionStatusUpdate update) {
		listener.statusUpdate(this, update.sessionStatus());
	}

	static class ListenerActor extends UntypedActor {

		private final LocalDispatcherClient client;

		public ListenerActor(LocalDispatcherClient client) {
			this.client = checkNotNull(client);
		}

		public void onReceive(Object msg) {
			if (msg instanceof DispatcherMessages.SessionStatusUpdate) {
				client.handleSessionStatusUpdate((DispatcherMessages.SessionStatusUpdate) msg);
			}
		}

		public static Props createActorProps(LocalDispatcherClient client) {
			return Props.create(ListenerActor.class, client);
		}
	}
}
