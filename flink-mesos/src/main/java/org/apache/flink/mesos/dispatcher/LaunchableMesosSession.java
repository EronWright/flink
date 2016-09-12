package org.apache.flink.mesos.dispatcher;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.dispatcher.types.SessionDefaults;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.runtime.clusterframework.MesosApplicationMasterRunner;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.TaskRequestBase;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredJobMasterParameters;
import org.apache.mesos.Protos;
import scala.Option;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

import static org.apache.flink.mesos.Utils.label;
import static org.apache.flink.mesos.Utils.labels;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.Utils.ranges;
import static org.apache.flink.mesos.Utils.scalar;
import static org.apache.flink.mesos.Utils.uri;
import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_CLASSPATH;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_CLIENT_USERNAME;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_DYNAMIC_PROPERTIES;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_FLINK_CONTAINER_ID;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_SLOTS;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_TM_COUNT;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.ENV_TM_MEMORY;
import static org.apache.mesos.Protos.TaskState.TASK_RUNNING;

/**
 * Specifies how to launch a Mesos session.
 */
public class LaunchableMesosSession implements LaunchableTask {

	/**
	 * The set of configuration keys to be dynamically configured with a port allocated from Mesos.
	 */
	private static final String[] APPMASTER_PORT_KEYS = {
		ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
		ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
		ConfigConstants.BLOB_SERVER_PORT,
		ConfigConstants.MESOS_ARTIFACT_SERVER_PORT_KEY};

	private static final String[] APPMASTER_PORT_ENVS = {
//		MesosConfigKeys.ENV_LIBPROCESS_PORT
	};

	private static final String LABEL_SESSIONID = "SessionID";

	private final MesosArtifactResolver resolver;

	private final SessionDefaults sessionDefaults;

	//private final MesosAppMasterParameters params;
	private final SessionParameters params;

	private final Protos.TaskInfo.Builder template;
	private final Protos.TaskID taskID;
	private final JobMasterRequest taskRequest;

	private final Configuration dynamicProperties;

	/**
	 * Construct a launchable Mesos session.
	 *
	 * @param params   the TM parameters such as memory, cpu to acquire.
	 * @param template a template for the TaskInfo to be constructed at launch time.
	 * @param taskID   the taskID for this worker.
	 */
	public LaunchableMesosSession(
		MesosArtifactResolver artifactResolver,
		SessionDefaults sessionDefaults,
		SessionParameters params,
		Protos.TaskInfo.Builder template,
		Protos.TaskID taskID) {
		this.resolver = artifactResolver;
		this.sessionDefaults = sessionDefaults;
		this.params = params;
		this.template = template;
		this.taskID = taskID;
		this.taskRequest = new JobMasterRequest();

		this.dynamicProperties = new Configuration();
	}

	@Override
	public Protos.TaskID taskID() {
		return taskID;
	}

	@Override
	public TaskRequest taskRequest() {
		return taskRequest;
	}

	class JobMasterRequest extends TaskRequestBase {
		public JobMasterRequest() {
			super(taskID);
		}

		@Override
		public double getCPUs() {
			return params.jmProfile().cpus();
		}

		@Override
		public double getMemory() {
			return params.jmProfile().mem();
		}

		@Override
		public int getPorts() {
			return APPMASTER_PORT_KEYS.length + APPMASTER_PORT_ENVS.length;
		}
	}

	/**
	 * Get the dynamic properties associated with the launchable task.
	 * <p>
	 * Dynamic properties are computed and supplied to the Mesos task at
	 * launch time (for example, IPC ports).
	 *
	 * @return a read-only view of the properties.
	 */
	public Configuration dynamicProperties() {
		return dynamicProperties.clone();
	}

	/**
	 * Construct the TaskInfo needed to launch the session.
	 *
	 * @param slaveId    the assigned slave.
	 * @param assignment the assignment details.
	 * @return a fully-baked TaskInfo.
	 */
	@Override
	public Protos.TaskInfo launch(Protos.SlaveID slaveId, TaskAssignmentResult assignment) {

		// specialize the TaskInfo template with assigned resources, environment variables, etc
		final Protos.TaskInfo.Builder taskInfo = template
			.clone()
			.setName("(placeholder) Session Name")
			.setSlaveId(slaveId)
			.setTaskId(taskID)
			.setName(taskID.getValue())
			.setLabels(labels(label(LABEL_SESSIONID, params.sessionID().toString())))
			.addResources(scalar("cpus", assignment.getRequest().getCPUs()))
			.addResources(scalar("mem", assignment.getRequest().getMemory()));

		final Protos.CommandInfo.Builder cmdBuilder = taskInfo.getCommandBuilder();
		final Protos.Environment.Builder environmentBuilder = cmdBuilder.getEnvironmentBuilder();

		// use the assigned ports
		if (assignment.getAssignedPorts().size() <
			(APPMASTER_PORT_KEYS.length + APPMASTER_PORT_ENVS.length)) {
			throw new IllegalArgumentException("unsufficient # of ports assigned");
		}
		Iterator<Integer> assignedPorts = assignment.getAssignedPorts().iterator();
		for (String key : APPMASTER_PORT_KEYS) {
			int port = assignedPorts.next();
			taskInfo.addResources(ranges("ports", range(port, port)));
			dynamicProperties.setInteger(key, port);
		}
		for (String env : APPMASTER_PORT_ENVS) {
			int port = assignedPorts.next();
			taskInfo.addResources(ranges("ports", range(port, port)));
			environmentBuilder.addVariables(
				variable(env, Integer.toString(port)));
		}

		// override the ZK namespace
		String zkNamespace = ZKPaths.makePath(sessionDefaults.getSessionRootNamespace(),
			this.params.sessionID().toString());
		dynamicProperties.setString(ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY, zkNamespace);

		// propagate the Mesos task ID to the session
		environmentBuilder
			.addVariables(variable(ENV_FLINK_CONTAINER_ID, taskInfo.getTaskId().getValue()));

		// set the security context
		environmentBuilder
			.addVariables(variable(ENV_CLIENT_USERNAME, params.username()));

		// process the artifacts
		StringBuilder classPathBuilder = new StringBuilder();
		StringBuilder tmShipFileList = new StringBuilder();

		// flink.jar
		Option<URL> flinkJarURL = resolver.resolve(params.sessionID(), new Path("flink.jar"));
		if (flinkJarURL.isEmpty()) {
			throw new RuntimeException("unable to resolve flink.jar");
		}
		cmdBuilder.addUris(uri(flinkJarURL.get(), true));
		classPathBuilder.append("flink.jar").append(File.pathSeparator);

		// flink-conf.yaml
		Option<URL> confURL = resolver.resolve(params.sessionID(), new Path("flink-conf.yaml"));
		if (confURL.isEmpty()) {
			throw new RuntimeException("unable to resolve flink-conf.yaml");
		}
		cmdBuilder.addUris(uri(confURL.get(), true));
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);

		// user artifacts
		for (SessionParameters.Artifact file : params.artifacts()) {
			Option<URL> url = resolver.resolve(params.sessionID(), new Path(file.remotePath()));

			// provide the artifact to the JobMaster
			// Note: Mesos 1.0+ provides an output_file field to specify the sandbox filename.
			// earlier versions use the last component of the path as the name.
			cmdBuilder.addUris(uri(url.get(), file.cacheable()));

			// instruct the JobMaster to ship the artifact to the TM also
			tmShipFileList.append(file.remotePath()).append(',');

			// generate the JM classpath
			classPathBuilder.append(file.remotePath()).append(File.pathSeparator);
		}
		environmentBuilder
			.addVariables(variable(ENV_CLASSPATH, classPathBuilder.toString()))
			.addVariables(variable(ENV_FLINK_CLASSPATH, classPathBuilder.toString()))
			.addVariables(variable(ENV_CLIENT_SHIP_FILES, tmShipFileList.toString()));

		// set the command
		Option<URL> logbackURL = resolver.resolve(params.sessionID(), new Path("logback.xml"));
		Option<URL> log4jURL = resolver.resolve(params.sessionID(), new Path("log4j.properties"));
		ContaineredJobMasterParameters containeredParams = ContaineredJobMasterParameters.create(
			params.configuration(),
			(long) assignment.getRequest().getMemory());
		String launchCommand = BootstrapTools.getJobMasterShellCommand(
			params.configuration(),
			containeredParams,
			".", ".",
			logbackURL.isDefined(), log4jURL.isDefined(),
			MesosApplicationMasterRunner.class);
		cmdBuilder.setValue(launchCommand);

		// propagate the TM resource profile
		environmentBuilder
			.addVariables(variable(ENV_SLOTS, String.valueOf(params.slots())))
			.addVariables(variable(ENV_TM_COUNT, String.valueOf(params.tmCount())))
			.addVariables(variable(ENV_TM_MEMORY, String.valueOf((int) params.tmProfile().mem())));

		dynamicProperties.setString(ConfigConstants.MESOS_RESOURCEMANAGER_TASKS_CPUS,
			String.valueOf(params.tmProfile().cpus()));

		// propagate the dynamic configuration properties to the session
		String dynamicPropertiesEncoded = BootstrapTools.encodeDynamicProperties(dynamicProperties);
		environmentBuilder
			.addVariables(variable(ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded));

		return taskInfo.build();
	}

	// ----- UTILS

	/**
	 * Generate the effective client configuration for the given session.
	 * <p>
	 * The effective configuration is a product of the user-supplied session parameters,
	 * dynamic properties supplied by the dispatcher (e.g. port assignments), and
	 * discovered address information.
	 *
	 * @return a Flink configuration instance.
	 */
	public static Configuration getClientConfiguration(
		SessionParameters params,
		Configuration dynamicProperties,
		Option<Protos.TaskStatus> taskStatus) {

		Configuration clientConfiguration = params.configuration().clone();
		clientConfiguration.addAll(dynamicProperties);

		if (taskStatus.isDefined() && taskStatus.get().getState() == TASK_RUNNING) {
			// running tasks have a network_info providing their actual IP address
			if (taskStatus.get().hasContainerStatus()) {
				Protos.ContainerStatus container = taskStatus.get().getContainerStatus();

				if (container.getNetworkInfosCount() >= 1 &&
					container.getNetworkInfos(0).getIpAddressesCount() >= 1) {
					clientConfiguration.setString(
						ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
						container.getNetworkInfos(0).getIpAddresses(0).getIpAddress());
				}
			}
		}

		return clientConfiguration;
	}

	@Override
	public String toString() {
		return "LaunchableMesosSession{" +
			"taskID=" + taskID +
			"taskRequest=" + taskRequest +
			'}';
	}
}
