package org.apache.flink.mesos.runtime.clusterframework;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.cli.FlinkMesosSessionCli;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.Utils.ranges;
import static org.apache.flink.mesos.Utils.scalar;

/**
 * Specifies the launch context (requirements, environment) for a specific worker.
 */
public class MesosWorkerLaunchContext implements TaskRequest {

	/**
	 * The set of configuration keys to be dynamically configured with a port allocated from Mesos.
	 */
	private static String[] TM_PORT_KEYS = {
		"taskmanager.rpc.port",
		"taskmanager.data.port" };

	final AtomicReference<TaskRequest.AssignedResources> assignedResources = new AtomicReference<>();

	private MesosTaskManagerParameters params;
	private Protos.TaskInfo.Builder template;
	private Protos.TaskID taskID;

	public MesosWorkerLaunchContext(MesosTaskManagerParameters params, Protos.TaskInfo.Builder template, Protos.TaskID taskID) {
		this.params = params;
		this.template = template;
		this.taskID = taskID;
	}

	@Override
	public String getId() { return taskID.getValue(); }
	@Override
	public String taskGroupName() { return ""; }
	@Override
	public double getCPUs() { return params.cpus(); }
	@Override
	public double getMemory() { return params.containeredParameters().taskManagerTotalMemoryMB(); }
	@Override
	public double getNetworkMbps() { return 0.0; }
	@Override
	public double getDisk() {
		return 0.0;
	}
	@Override
	public int getPorts() { return TM_PORT_KEYS.length; }

	@Override
	public Map<String, NamedResourceSetRequest> getCustomNamedResources() { return Collections.emptyMap(); }

	@Override
	public List<? extends ConstraintEvaluator> getHardConstraints() { return null; }

	@Override
	public List<? extends VMTaskFitnessCalculator> getSoftConstraints() { return null; }

	@Override
	public void setAssignedResources(AssignedResources assignedResources) {
		this.assignedResources.set(assignedResources);
	}

	@Override
	public AssignedResources getAssignedResources() { return assignedResources.get(); }

	public LaunchCoordinator.TaskSpecification toTaskSpecification() {
		return new TaskSpecification(this);
	}

	public class TaskSpecification extends LaunchCoordinator.TaskSpecification {

		TaskRequest taskRequest;

		TaskSpecification(TaskRequest taskRequest) {
			this.taskRequest = taskRequest;
		}

		@Override
		public TaskRequest taskRequest() {
			return taskRequest;
		}

		@Override
		public LaunchCoordinator.TaskBuilder launch() {

			// specialize the provided template
			Protos.TaskInfo.Builder taskInfo = template
				.clone()
				.setTaskId(taskID)
				.setName(taskID.getValue());

			return new TaskBuilder(taskInfo);
		}
	}

	public static class TaskBuilder extends LaunchCoordinator.TaskBuilder {

		private final Configuration dynamicProperties;
		private final Protos.TaskInfo.Builder taskInfo;

		TaskBuilder(Protos.TaskInfo.Builder taskInfo) {
			this.taskInfo = taskInfo;
			dynamicProperties = new Configuration();
		}

		@Override
		public TaskBuilder setSlaveID(Protos.SlaveID slaveId) {
			taskInfo.setSlaveId(slaveId);
			return this;
		}

		@Override
		public TaskBuilder setTaskAssignmentResult(TaskAssignmentResult assignment) {

			// use basic resources
			taskInfo
				.addResources(scalar("cpus", assignment.getRequest().getCPUs()))
				.addResources(scalar("mem", assignment.getRequest().getMemory()));
				//.addResources(scalar("disk", assignment.getRequest.getDisk).setRole("Flink"))

			// use the assigned ports for the TM
			if(assignment.getAssignedPorts().size() != TM_PORT_KEYS.length) {
				throw new IllegalArgumentException("unsufficient # of ports assigned");
			}
			for(int i = 0; i < TM_PORT_KEYS.length; i++) {
				int port = assignment.getAssignedPorts().get(i);
				String key = TM_PORT_KEYS[i];
				taskInfo.addResources(ranges("ports", range(port, port)));
				dynamicProperties.setInteger(key, port);
			}

			return this;
		}

		@Override
		public Protos.TaskInfo build() {

			// propagate the Mesos task ID to the TM
			taskInfo.getCommandBuilder().getEnvironmentBuilder()
				.addVariables(variable(MesosConfigKeys.ENV_FLINK_CONTAINER_ID, taskInfo.getTaskId().getValue()));

			// propagate the dynamic configuration properties to the TM
			String dynamicPropertiesEncoded = FlinkMesosSessionCli.encodeDynamicProperties(dynamicProperties);
			taskInfo.getCommandBuilder().getEnvironmentBuilder()
				.addVariables(variable(MesosConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded));

			return taskInfo.build();
		}
	}
}
