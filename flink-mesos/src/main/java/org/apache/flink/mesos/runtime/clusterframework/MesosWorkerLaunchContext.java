package org.apache.flink.mesos.runtime.clusterframework;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Specifies the launch context (requirements, environment) for a specific worker.
 */
public class MesosWorkerLaunchContext implements TaskRequest {

	final AtomicReference<TaskRequest.AssignedResources> assignedResources = new AtomicReference<>();

	private Protos.TaskInfo.Builder template;
	private Protos.TaskID taskID;

	public MesosWorkerLaunchContext(Protos.TaskInfo.Builder template, Protos.TaskID taskID) {
		this.template = template;
		this.taskID = taskID;
	}

	@Override
	public String getId() { return taskID.getValue(); }
	@Override
	public String taskGroupName() { return ""; }
	@Override
	public double getCPUs() { return 1.0; }
	@Override
	public double getMemory() { return 256.0; }
	@Override
	public double getNetworkMbps() { return 0.0; }
	@Override
	public double getDisk() {
		return 7 * 1024;
	}
	@Override
	public int getPorts() { return 2; }

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

	/**
	 * Gets a Mesos TaskInfo builder for the given task.
	 *
	 * The launch context may return a partially-initialized builder.
     */
	public Protos.TaskInfo.Builder taskInfo() {
		Protos.TaskInfo.Builder taskInfo = template
			.clone()
			.setTaskId(taskID)
			.setName(taskID.getValue());

		taskInfo.getCommandBuilder().getEnvironmentBuilder()
			.addVariables(variable(MesosConfigKeys.ENV_FLINK_CONTAINER_ID, taskID.getValue()));

		String[] dynamicProperties = new String[] {
			"taskmanager.rpc.port=0", //9870
			"taskmanager.data.port=0" //9871
		};

		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties,
			CliFrontend.YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		taskInfo.getCommandBuilder().getEnvironmentBuilder()
			.addVariables(variable(MesosConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded));

		return taskInfo;
	}

	private static Protos.Environment.Variable variable(String name, String value) {
		return Protos.Environment.Variable.newBuilder()
			.setName(name)
			.setValue(value)
			.build();
	}

	public LaunchCoordinator.TaskSpecification toTaskSpecification() {
		return new LaunchCoordinator.TaskSpecification(this, taskInfo());
	}
}
