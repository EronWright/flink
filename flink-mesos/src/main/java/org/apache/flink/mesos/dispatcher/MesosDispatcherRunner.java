/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.dispatcher;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.types.SessionDefaults;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.Map;

/**
 * Runs the Mesos dispatcher.
 * <p>
 * The dispatcher process contains:
 * - MesosSessionStore - an implementation of a session store.
 * - MesosDispatcherBackend - schedules sessions as Mesos tasks.
 * - MesosArtifactServer - serve artifacts to launch a Mesos JobMaster.
 */
public class MesosDispatcherRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(MesosDispatcherRunner.class);

	/**
	 * The process environment variables
	 */
	private static final Map<String, String> ENV = System.getenv();

	/**
	 * The exit code returned if the initialization of the application master failed
	 */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/**
	 * The exit code returned if the process exits because a critical actor died
	 */
	private static final int ACTOR_DIED_EXIT_CODE = 32;

	public static void main(String[] args) {

		EnvironmentInformation.logEnvironmentInfo(LOG, "Mesos Dispatcher", args);
		SignalHandler.register(LOG);

		// run and exit with the proper return code
		int returnCode = new MesosDispatcherRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the Mesos AppMaster. Obtains user group
	 * information and calls the main work method {@link #runPrivileged()} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			return runPrivileged();

		} catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("Mesos AppMaster initialization failed", t);
			return INIT_ERROR_EXIT_CODE;
		}
	}

	/**
	 * The main work method, must run as a privileged action.
	 *
	 * @return The return code for the Java process.
	 */
	protected int runPrivileged() throws Exception {

		ActorSystem actorSystem = null;
		try {
			// read the Flink configuration
			Configuration config = GlobalConfiguration.loadConfiguration();

			// start a local-only actor system
			actorSystem = AkkaUtils.createLocalActorSystem(config);
			final String dispatcherHostname = InetAddress.getLocalHost().getHostName();
			LOG.info("Actor system started (local).");

			LeaderElectionService leaderElectionService = new StandaloneLeaderElectionService();

			MesosConfiguration mesosConfig =
				DispatcherUtils.createMesosConfig(config, dispatcherHostname);

			Protos.TaskInfo.Builder jmTaskInfo = createJobMasterTaskInfoTemplate();

			MesosSessionStore sessionStore = DispatcherUtils.createSessionStore(config);

			SessionDefaults sessionDefaults = createSessionDefaults(config);

			// artifacts
			Path flinkJarPath = new Path(new File("flink.jar").toURI());
			MesosArtifactServer artifactServer =
				new MesosArtifactServer("placeholder", dispatcherHostname, 0);
			SessionArtifactServer sessionArtifactServer = new SessionArtifactServer(
				artifactServer, flinkJarPath);

			Props dispatcherProps = MesosDispatcherBackend.createActorProps(
				MesosDispatcherBackend.class,
				config, mesosConfig, jmTaskInfo, sessionStore, sessionDefaults,
				leaderElectionService, sessionArtifactServer,
				ActorRef.noSender(),
				LOG);

			ActorRef dispatcher = actorSystem.actorOf(dispatcherProps, "Mesos_Dispatcher");

			LOG.debug("Starting process reapers for MesosDispatcher");
			actorSystem.actorOf(
				Props.create(ProcessReaper.class, dispatcher, LOG, ACTOR_DIED_EXIT_CODE),
				"Mesos_Dispatcher_Process_Reaper");
		} catch (Throwable t) {
			LOG.error("Mesos Dispatcher initialization failed", t);

			if (actorSystem != null) {
				actorSystem.shutdown();
			}

			return INIT_ERROR_EXIT_CODE;
		}

		// everything started, we can wait until all is done or the process is killed
		LOG.info("Mesos Dispatcher started");

		// wait until everything is done
		actorSystem.awaitTermination();

		return 0;
	}

	private static SessionDefaults createSessionDefaults(Configuration flinkConfig) {
		String dispatcherNamespace = flinkConfig.getString(
			ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY,
			ConfigConstants.DEFAULT_ZOOKEEPER_NAMESPACE_KEY);

		SessionDefaults defaults = new SessionDefaults(
			ZKPaths.makePath(dispatcherNamespace, "sessions"));

		return defaults;
	}

	private static Protos.TaskInfo.Builder createJobMasterTaskInfoTemplate() {
		Protos.TaskInfo.Builder info = Protos.TaskInfo.newBuilder();
		Protos.CommandInfo.Builder cmd = Protos.CommandInfo.newBuilder();
		return info;
	}

}
