package org.apache.flink.mesos.dispatcher;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.cli.FlinkMesosSessionCli;
import org.apache.flink.mesos.dispatcher.types.SessionParameters;
import org.apache.flink.mesos.runtime.clusterframework.MesosApplicationMasterRunner;
import org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.TaskRequestBase;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredJobMasterParameters;
import org.apache.mesos.Protos;
import scala.Option;

import java.io.File;
import java.net.URL;

import static org.apache.flink.mesos.Utils.*;
import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.Utils.uri;
import static org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys.*;

/**
 * Specifies how to launch a Mesos session.
 */
public class LaunchableMesosSession implements LaunchableTask {

	/**
	 * The set of configuration keys to be dynamically configured with a port allocated from Mesos.
	 */
	private static String[] APPMASTER_PORT_KEYS = {
		"jobmanager.rpc.port",
		"jobmanager.web.port",
		"blob.server.port",
		"mesos.resourcemanager.artifactserver.port",
		"mesos.resourcemanager.scheduler.port"};

	MesosArtifactResolver artifactResolver;

	//private final MesosAppMasterParameters params;
	private final SessionParameters params;

	private final Protos.TaskInfo.Builder template;
	private final Protos.TaskID taskID;
	private final JobMasterRequest taskRequest;

	/**
	 * Construct a launchable Mesos session.
	 * @param params the TM parameters such as memory, cpu to acquire.
	 * @param template a template for the TaskInfo to be constructed at launch time.
	 * @param taskID the taskID for this worker.
	 */
	public LaunchableMesosSession(MesosArtifactResolver artifactResolver, SessionParameters params, Protos.TaskInfo.Builder template, Protos.TaskID taskID) {
		this.artifactResolver = artifactResolver;
		this.params = params;
		this.template = template;
		this.taskID = taskID;
		this.taskRequest = new JobMasterRequest();
	}

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
			return APPMASTER_PORT_KEYS.length;
		}
	}

	/**
	 * Construct the TaskInfo needed to launch the session.
	 * @param slaveId the assigned slave.
	 * @param assignment the assignment details.
	 * @return a fully-baked TaskInfo.
	 */
	@Override
	public Protos.TaskInfo launch(Protos.SlaveID slaveId, TaskAssignmentResult assignment) {

		final Configuration dynamicProperties = new Configuration();

		// specialize the TaskInfo template with assigned resources, environment variables, etc
		final Protos.TaskInfo.Builder taskInfo = template
			.clone()
			.setName("(placeholder) Session Name")
			.setSlaveId(slaveId)
			.setTaskId(taskID)
			.setName(taskID.getValue())
			.addResources(scalar("cpus", assignment.getRequest().getCPUs()))
			.addResources(scalar("mem", assignment.getRequest().getMemory()));

		final Protos.CommandInfo.Builder cmdBuilder = taskInfo.getCommandBuilder();
		final Protos.Environment.Builder environmentBuilder = cmdBuilder.getEnvironmentBuilder();

		// use the assigned ports
		if (assignment.getAssignedPorts().size() != APPMASTER_PORT_KEYS.length) {
			throw new IllegalArgumentException("unsufficient # of ports assigned");
		}
		for (int i = 0; i < APPMASTER_PORT_KEYS.length; i++) {
			int port = assignment.getAssignedPorts().get(i);
			String key = APPMASTER_PORT_KEYS[i];
			taskInfo.addResources(ranges("ports", range(port, port)));
			dynamicProperties.setInteger(key, port);
		}

		// propagate the Mesos task ID to the session
		environmentBuilder
			.addVariables(variable(ENV_FLINK_CONTAINER_ID, taskInfo.getTaskId().getValue()))
			.addVariables(variable("_CLIENT_SESSION_ID", taskInfo.getTaskId().getValue()));

		// set the security context
		environmentBuilder
			.addVariables(variable(ENV_CLIENT_USERNAME, params.username()));

		// process the artifacts
		StringBuilder classPathBuilder = new StringBuilder();
		StringBuilder tmShipFileList = new StringBuilder();
//		for(SessionParameters.Artifact file : params.artifacts()) {
//			Protos.CommandInfo.URI uri = uri(file.url(), file.cacheable());
//			String localPath = getSandboxPath(uri);
//
//			// provide the artifact to the JobMaster
//			cmdBuilder.addUris(uri);
//
//			// instruct the JobMaster to ship the artifact to the TM also
//			tmShipFileList.append(localPath).append(',');
//
//			// generate the JM/TM classpath
//			classPathBuilder.append(localPath).append(File.pathSeparator);
//		}
//		environmentBuilder
//			.addVariables(variable(ENV_CLASSPATH, classPathBuilder.toString()))
//			.addVariables(variable(ENV_FLINK_CLASSPATH, classPathBuilder.toString()))
//			.addVariables(variable(ENV_CLIENT_SHIP_FILES, tmShipFileList.toString()));

		// flink.jar
		Option<URL> flinkJarURL = artifactResolver.resolve(params.jobID(), new Path("flink.jar"));
		if(flinkJarURL.isEmpty()) {
			throw new RuntimeException("unable to resolve flink.jar");
		}
		cmdBuilder.addUris(uri(flinkJarURL.get(), true));
		classPathBuilder.append("flink.jar").append(File.pathSeparator);

		// user artifacts
		for(SessionParameters.Artifact file : params.artifacts()) {

			Option<URL> url = artifactResolver.resolve(params.jobID(), new Path(file.remotePath()));

			// provide the artifact to the JobMaster
			cmdBuilder.addUris(uri(url.get(), file.cacheable()));

			// instruct the JobMaster to ship the artifact to the TM also
			tmShipFileList.append(file.localPath()).append(',');

			// generate the JM/TM classpath
			classPathBuilder.append(file.localPath()).append(File.pathSeparator);
		}
		environmentBuilder
			.addVariables(variable(ENV_CLASSPATH, classPathBuilder.toString()))
			.addVariables(variable(ENV_FLINK_CLASSPATH, classPathBuilder.toString()))
			.addVariables(variable(ENV_CLIENT_SHIP_FILES, tmShipFileList.toString()));

		// set the command
		Option<URL> logbackURL = artifactResolver.resolve(params.jobID(), new Path("logback.xml"));
		Option<URL> log4jURL = artifactResolver.resolve(params.jobID(), new Path("log4j.properties"));

//		boolean hasLogback = findArtifactByName(cmdBuilder, "logback.xml").isDefined();
//		boolean hasLog4j = findArtifactByName(cmdBuilder, "log4j.properties").isDefined();
		ContaineredJobMasterParameters containeredParams = ContaineredJobMasterParameters.create(
				params.configuration(),
				(long) assignment.getRequest().getMemory());
		String launchCommand = BootstrapTools.getJobMasterShellCommand(
			params.configuration(),
			containeredParams,
			".", ".", logbackURL.isDefined(), log4jURL.isDefined(), MesosApplicationMasterRunner.class);
		cmdBuilder.setValue(launchCommand);

		// propagate the TM resource profile
		environmentBuilder
			.addVariables(variable(ENV_SLOTS, String.valueOf(params.slots())))
			.addVariables(variable(ENV_TM_COUNT, String.valueOf(params.tmCount())))
			.addVariables(variable(ENV_TM_MEMORY, String.valueOf((int)params.tmProfile().mem())));
		dynamicProperties.setString(ConfigConstants.MESOS_RESOURCEMANAGER_TASKS_CPUS,
			String.valueOf(params.tmProfile().cpus()));

		// propagate the dynamic configuration properties to the session
		String dynamicPropertiesEncoded = FlinkMesosSessionCli.encodeDynamicProperties(dynamicProperties);
		environmentBuilder
			.addVariables(variable(ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded));

		return taskInfo.build();
	}

	// ----- UTILS

	/**
	 * Get the local path (in the task sandbox) of the given artifact.
	 * @param uri the artifact information.
	 * @return the local path to which the Mesos fetcher will copy the artifact.
     */
	@Deprecated
	public static String getSandboxPath(Protos.CommandInfo.URI uri) {

		// note: Mesos 1.0+ provides an output_file field to control the filename in the sandbox.
		// earlier versions use the last component of the path as the name.

		final String path = new Path(uri.getValue()).getName();
		return path;
	}


	@Deprecated
	public static Option<Protos.CommandInfo.URI> findArtifactByName(Protos.CommandInfoOrBuilder cmd, String name) {
		for(Protos.CommandInfo.URI a : cmd.getUrisList()) {
			if(name.equals(new Path(a.getValue()).getName())) return Option.apply(a);
		}
		return Option.empty();
	}

	@Override
	public String toString() {
		return "LaunchableMesosSession{" +
			"taskID=" + taskID +
			"taskRequest=" + taskRequest +
			'}';
	}
}
