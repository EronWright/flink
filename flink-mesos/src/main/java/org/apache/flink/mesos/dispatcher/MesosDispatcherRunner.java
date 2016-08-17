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
import com.typesafe.config.Config;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.process.ProcessReaper;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs the Mesos dispatcher.
 *
 * The dispatcher process contains:
 *  - DispatcherFrontend - handles requests for sessions.
 *  - SessionStore - an implementation of a session store.
 *  - MesosDispatcherBackend - schedules sessions as Mesos tasks.
 *  - MesosArtifactServer - serve artifacts to launch a Mesos AppMaster.
 */
public class MesosDispatcherRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(MesosDispatcherRunner.class);

	/** The process environment variables */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/** The exit code returned if the process exits because a critical actor died */
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

		}
		catch (Throwable t) {
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
			final String dispatcherHostname = InetAddress.getLocalHost().getHostName();
			Config akkaConfig = AkkaUtils.getAkkaConfig(config, Option.<Tuple2<String,Object>>empty());
			LOG.debug("Using akka configuration\n {}", akkaConfig);
			actorSystem = AkkaUtils.createActorSystem(akkaConfig);
			LOG.info("Actor system started (local).");


			LeaderElectionService leaderElectionService = new StandaloneLeaderElectionService();

			MesosConfiguration mesosConfig = DispatcherUtils.createMesosConfig(config, dispatcherHostname);

			Protos.TaskInfo.Builder jmTaskInfo = createJobMasterTaskInfoTemplate();

			MesosSessionStore sessionStore = DispatcherUtils.createSessionStore(config);

			initializeSessionStore(sessionStore);

			MesosArtifactServer artifactServer = new MesosArtifactServer("placeholder", dispatcherHostname, 0);

			Props dispatcherProps = MesosDispatcherBackend.createActorProps(
				MesosDispatcherBackend.class,
				config, mesosConfig, jmTaskInfo, sessionStore, leaderElectionService, artifactServer,
				new Path("flink.jar"),
				LOG);

			ActorRef dispatcher = actorSystem.actorOf(dispatcherProps, "Mesos_Dispatcher");

			LOG.debug("Starting process reapers for MesosDispatcher");
			actorSystem.actorOf(
				Props.create(ProcessReaper.class, dispatcher, LOG, ACTOR_DIED_EXIT_CODE),
				"Mesos_Dispatcher_Process_Reaper");
		}
		catch(Throwable t) {
			LOG.error("Mesos Dispatcher initialization failed", t);

			if(actorSystem != null) {
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


	private static void initializeSessionStore(MesosSessionStore store) throws Exception {

		List<SessionParameters.Artifact> artifacts = new ArrayList<>();
		//artifacts.add(new SessionParameters.Artifact(new URL("http://127.0.0.1:8000/flink-conf.yaml"), false));
		//artifacts.add(new SessionParameters.Artifact(new URL("http://127.0.0.1:8000/flink.jar"), false));

		Path workingDir = FileSystem.getLocalFileSystem().getWorkingDirectory();
		artifacts.add(new SessionParameters.Artifact(new Path(workingDir, "log4j.properties"), "log4j.properties", false));
		artifacts.add(new SessionParameters.Artifact(new Path(workingDir, "log4j-1.2.17.jar"), "log4j-1.2.17.jar", false));
		artifacts.add(new SessionParameters.Artifact(new Path(workingDir, "slf4j-log4j12-1.7.7.jar"), "slf4j-log4j12-1.7.7.jar", false));

		SessionParameters params = new SessionParameters(
			UserGroupInformation.getCurrentUser().getShortUserName(),
			new SessionParameters.ResourceProfile(1.0, 768.0),
			new SessionParameters.ResourceProfile(1.5, 3000.0),
			1,
			1,
			JobID.generate(),
			artifacts,
			null
		);

		MesosSessionStore.Session session = MesosSessionStore.Session.newTask(params, store.newTaskID());

		store.putSession(session);
	}

	private static Protos.TaskInfo.Builder createJobMasterTaskInfoTemplate() {
		Protos.TaskInfo.Builder info = Protos.TaskInfo.newBuilder();
		Protos.CommandInfo.Builder cmd = Protos.CommandInfo.newBuilder();
		return info;
	}

}
