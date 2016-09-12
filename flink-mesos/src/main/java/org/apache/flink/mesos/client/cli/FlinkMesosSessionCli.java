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

package org.apache.flink.mesos.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.SessionArtifactHelper;
import org.apache.flink.client.deployment.SessionArtifactHelper.ShipFile;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.client.deployment.DispatcherClient;
import org.apache.flink.mesos.client.deployment.DispatcherClientListener;
import org.apache.flink.mesos.client.deployment.LocalDispatcherClient;
import org.apache.flink.mesos.client.program.MesosClusterClient;
import org.apache.flink.mesos.dispatcher.types.SessionID;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.dispatcher.types.SessionParameters.ResourceProfile;
import org.apache.flink.mesos.dispatcher.types.SessionStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.client.cli.CliFrontendParser.ADDRESS_OPTION;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY;

/**
 * Class handling the command line interface to the Mesos session.
 */
public class FlinkMesosSessionCli implements CustomCommandLine<MesosClusterClient> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkMesosSessionCli.class);

	//------------------------------------ Constants   -------------------------

	/**
	 * The id for the CommandLine interface
	 */
	private static final String ID = "mesos-cluster";

	//------------------------------------ Command Line argument options -------------------------
	// the prefix transformation is used by the CliFrontend static constructor.
	private final Option QUERY;
	// --- or ---
	private final Option SESSION_ID;
	// --- or ---
	private final Option ROLE;
	private final Option SHIP_PATH;
	private final Option FLINK_JAR;
	private final Option JM_MEMORY;
	private final Option TM_MEMORY;
	private final Option TM_COUNT;
	private final Option SLOTS;
	private final Option DETACHED;
	private final Option ZOOKEEPER_NAMESPACE;
	private final Option NAME;

	private final Options ALL_OPTIONS;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * -D fs.overwrite-files=true  -D taskmanager.network.numberOfBuffers=16368
	 */
	private final Option DYNAMIC_PROPERTIES;

	private final boolean acceptInteractiveInput;

	public FlinkMesosSessionCli(String shortPrefix, String longPrefix) {
		this(shortPrefix, longPrefix, true);
	}

	public FlinkMesosSessionCli(String shortPrefix, String longPrefix, boolean acceptInteractiveInput) {
		this.acceptInteractiveInput = acceptInteractiveInput;

		QUERY = new Option(shortPrefix + "q", longPrefix + "query", false, "Display Mesos information");
		SESSION_ID = new Option(shortPrefix + "id", longPrefix + "sessionId", true, "Attach to running Mesos session");
		ROLE = new Option(shortPrefix + "r", longPrefix + "role", true, "Mesos role for the cluster");
		SHIP_PATH = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		FLINK_JAR = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		JM_MEMORY = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		TM_MEMORY = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		TM_COUNT = new Option(shortPrefix + "n", longPrefix + "taskManagerCount", true, "Number of TaskManager instances to allocate");
		SLOTS = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		DYNAMIC_PROPERTIES = new Option(shortPrefix + "D", true, "Dynamic properties");
		DETACHED = new Option(shortPrefix + "d", longPrefix + "detached", false, "Start detached");
		NAME = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the framework on Mesos");
		ZOOKEEPER_NAMESPACE = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");

		ALL_OPTIONS = new Options();
		ALL_OPTIONS.addOption(FLINK_JAR);
		ALL_OPTIONS.addOption(JM_MEMORY);
		ALL_OPTIONS.addOption(TM_MEMORY);
		ALL_OPTIONS.addOption(TM_COUNT);
		ALL_OPTIONS.addOption(ROLE);
//		ALL_OPTIONS.addOption(QUERY);
		ALL_OPTIONS.addOption(SHIP_PATH);
		ALL_OPTIONS.addOption(SLOTS);
		ALL_OPTIONS.addOption(DYNAMIC_PROPERTIES);
//		ALL_OPTIONS.addOption(DETACHED);
		ALL_OPTIONS.addOption(NAME);
//		ALL_OPTIONS.addOption(SESSION_ID);
		ALL_OPTIONS.addOption(ZOOKEEPER_NAMESPACE);
	}

	public static void main(String[] args) {
		FlinkMesosSessionCli cli = new FlinkMesosSessionCli("", ""); // no prefix for the Mesos session
		System.exit(cli.run(args));
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		String jobManagerOption = commandLine.getOptionValue(ADDRESS_OPTION.getOpt(), null);
		boolean hasDispatcher = ID.equals(jobManagerOption);
		boolean hasSessionId = commandLine.hasOption(SESSION_ID.getOpt());
		return hasDispatcher || hasSessionId;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		for (Object option : ALL_OPTIONS.getOptions()) {
			baseOptions.addOption((Option) option);
		}
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(SESSION_ID);
	}

	@Override
	public MesosClusterClient retrieveCluster(CommandLine cmd, Configuration config) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Unable to resume a Mesos session");
	}

	@Override
	public MesosClusterClient createCluster(String applicationName, CommandLine cmd, Configuration config) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Unable to create a Mesos session");
	}

	/**
	 * Implementation of createCluster.
	 */
	private class CreateClusterCommand implements DispatcherClientListener {

		private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

		private final CommandLine cmd;
		private final Configuration cliConfig;

		private final DispatcherClient dispatcherClient;

		public CreateClusterCommand(CommandLine cmd, Configuration cliConfig) {

			this.cmd = cmd;
			this.cliConfig = cliConfig;

			// read hadoop configuration
			hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

			// create local dispatcher
			Path localFlinkJarPath = getLocalFlinkJarPath(cmd);
			try {
				dispatcherClient = new LocalDispatcherClient(this, cliConfig, localFlinkJarPath);
			} catch (Exception e) {
				throw new IllegalStateException("Unable to create local dispatcher");
			}
		}

		public void close() {
			try {
				dispatcherClient.close();
			} catch (Exception e) {
				LOG.warn("unable to close the dispatcher client", e);
			}
		}

		public int execute() {
			try {
				UserGroupInformation.setConfiguration(hadoopConfiguration);
				UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

				if (UserGroupInformation.isSecurityEnabled()) {
					if (!ugi.hasKerberosCredentials()) {
						logAndSysout(
							"In secure mode. Please provide Kerberos credentials in order to authenticate. " +
								"You may use kinit to authenticate and request a TGT from the Kerberos server.");
						return 1;
					}
					return ugi.doAs(new PrivilegedExceptionAction<Integer>() {
						@Override
						public Integer run() throws Exception {
							return executeInternal();
						}
					});
				} else {
					return executeInternal();
				}

			} catch (Exception e) {
				throw new RuntimeException("Couldn't deploy Flink cluster", e);
			}
		}

		private Path getLocalFlinkJarPath(CommandLine cmd) {
			Path localJarPath;
			if (cmd.hasOption(FLINK_JAR.getOpt())) {
				String userPath = cmd.getOptionValue(FLINK_JAR.getOpt());
				if (!userPath.startsWith("file://")) {
					userPath = "file://" + userPath;
				}
				localJarPath = new Path(userPath);
			} else {
				LOG.info("No path for the flink jar passed. Using the location of "
					+ this.getClass() + " to locate the jar");
				String encodedJarPath =
					this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
				try {
					// we have to decode the url encoded parts of the path
					String decodedPath = URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
					localJarPath = new Path(new File(decodedPath).toURI());
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath +
						" Please supply a path manually via the -" + FLINK_JAR.getOpt() + " option.");
				}
			}
			return localJarPath;
		}

		public int executeInternal() throws Exception {

			SessionID sessionID;
			try {
				SessionParameters params = createSessionParameters();
				sessionID = params.sessionID();
				System.out.println("Deploying session (" + sessionID + ")...");

				dispatcherClient.startSession(params);

			} catch (Exception e) {
				System.err.println("Error while deploying Mesos cluster: " + e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}

			// the dispatcher client listener will report status changes to System.out

			// wait for CTRL-C
			final CountDownLatch shutdownLatch = new CountDownLatch(1);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					shutdownLatch.countDown();
				}
			});
			shutdownLatch.await();

			return 0;
		}

		private SessionParameters createSessionParameters() throws Exception {

			SessionParameters.Builder params = dispatcherClient.newSession();

			// Configuration
			Configuration sessionConfig = cliConfig.clone();
			if (cmd.hasOption(DYNAMIC_PROPERTIES.getOpt())) {
				String[] dynamicProperties = cmd.getOptionValues(DYNAMIC_PROPERTIES.getOpt());
				Configuration dynamicConf = readDynamicPropertiesOpt(dynamicProperties);
				sessionConfig.addAll(dynamicConf);
			}

			if (cmd.hasOption(ZOOKEEPER_NAMESPACE.getOpt())) {
				String zkNamespace = cmd.getOptionValue(ZOOKEEPER_NAMESPACE.getOpt());
				sessionConfig.setString(HA_ZOOKEEPER_NAMESPACE_KEY, zkNamespace);
			}

			params.setConfiguration(sessionConfig);

			// Session name
			String name = "Flink Session";
			if (cmd.hasOption(NAME.getOpt())) {
				name = cmd.getOptionValue(NAME.getOpt());
			}
			params.setName(name);

			// username
			params.setUsername(UserGroupInformation.getCurrentUser().getShortUserName());

			// Job Manager profile
			double jmMemory = SessionParameters.DEFAULT_JM_PROFILE.mem();
			if (cmd.hasOption(JM_MEMORY.getOpt())) {
				jmMemory = Integer.valueOf(cmd.getOptionValue(JM_MEMORY.getOpt()));
			}
			params.setJmProfile(new ResourceProfile(
				SessionParameters.DEFAULT_JM_PROFILE.cpus(), jmMemory));

			// Task Manager count
			if (!cmd.hasOption(TM_COUNT.getOpt())) { // number of TMs is required option!
				LOG.error("Missing required argument {}", TM_COUNT.getOpt());
				printUsage();
				throw new IllegalArgumentException("Missing required argument " + TM_COUNT.getOpt());
			}
			int tmCount = Integer.valueOf(cmd.getOptionValue(TM_COUNT.getOpt()));
			params.setTmCount(tmCount);

			// Task Manager slots
			int slotsPerTM = 1;
			if (cmd.hasOption(SLOTS.getOpt())) {
				slotsPerTM = Integer.valueOf(cmd.getOptionValue(SLOTS.getOpt()));
			}
			params.setSlots(slotsPerTM);

			// Task Manager profile
			double tmMemory = SessionParameters.DEFAULT_TM_PROFILE.mem();
			if (cmd.hasOption(TM_MEMORY.getOpt())) {
				tmMemory = Integer.valueOf(cmd.getOptionValue(TM_MEMORY.getOpt()));
			}
			params.setTmProfile(new ResourceProfile(Math.max(slotsPerTM, 1), tmMemory));

			// artifacts
			SessionArtifactHelper artifactBuilder = new SessionArtifactHelper();

			if (cmd.hasOption(SHIP_PATH.getOpt())) {
				File shipDir = new File(cmd.getOptionValue(SHIP_PATH.getOpt()));
				if (shipDir.isDirectory()) {
					artifactBuilder.addAll(new ShipFile(shipDir, new Path(".")));
				} else {
					LOG.warn("Ship directory is not a directory. Ignoring it.");
				}
			}

			File configDir = new File(CliFrontend.getConfigurationDirectoryFromEnv());
			artifactBuilder.setConfigurationDirectory(configDir);

			if (System.getenv().containsKey(ENV_FLINK_LIB_DIR)) {
				File libDir = new File(System.getenv().get(ENV_FLINK_LIB_DIR));
				artifactBuilder.setLibDirectory(libDir);
			} else {
				LOG.warn("Environment variable '{}' not set; ship files should be provided manually.",
					ENV_FLINK_LIB_DIR);
			}

			List<ShipFile> shipFiles = artifactBuilder.build();
			shipFiles = SessionArtifactHelper.expand(shipFiles);
			List<SessionParameters.Artifact> artifacts = new ArrayList<>();
			for (ShipFile file : shipFiles) {
				artifacts.add(new SessionParameters.Artifact(
					new Path(file.getLocalPath().getAbsolutePath()), file.getRemotePath(), true));
			}
			params.setArtifacts(artifacts);

			return params.build();
		}

		@Override
		public void statusUpdate(DispatcherClient client, SessionStatus status) {
			Configuration config = status.getClientConfiguration();

			String jobManagerAddress =
				config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "") +
					":" + config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0);


			StringBuilder exampleArgs = new StringBuilder();
			System.out.println("\n--------------------------------------------------------------------------------");
			System.out.println(" Session Info:");
			System.out.println("    JobManager: " + jobManagerAddress);
			exampleArgs.append("-m ").append(jobManagerAddress).append(" ");

			if (HighAvailabilityMode.fromConfig(config) == HighAvailabilityMode.ZOOKEEPER) {
				System.out.println(" ZooKeeper Info:");
				String zkNamespace = config.getString(ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY, "(default)");
				System.out.println("    Namespace: " + zkNamespace);
				exampleArgs.append("-z ").append(zkNamespace).append(" ");
			}

			System.out.println("\nUse the above information to run a Flink program in your Mesos environment.");
			System.out.println("For example:");
			System.out.printf("  flink run %s$FLINK_HOME/examples/streaming/WordCount.jar\n", exampleArgs);
			System.out.println("\n--------------------------------------------------------------------------------");

		}
	}

	private static Configuration readDynamicPropertiesOpt(String[] dynamicProperties) {
		Configuration conf = new Configuration();
		for (String propLine : dynamicProperties) {
			if (propLine == null) {
				continue;
			}

			String[] kv = propLine.split("=");
			if (kv.length >= 2 && kv[0] != null && kv[1] != null && kv[0].length() > 0) {
				conf.setString(kv[0], kv[1]);
			}
		}
		return conf;
	}

	public int run(String[] args) {
		//
		//	Command Line Options
		//
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			printUsage();
			return 1;
		}

		String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
		Configuration cliConfig = GlobalConfiguration.loadConfiguration(configurationDirectory);

		CreateClusterCommand context = new CreateClusterCommand(cmd, cliConfig);
		try {
			return context.execute();
		} finally {
			context.close();
		}
	}

	private void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(TM_COUNT);
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}
}
