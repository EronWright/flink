package org.apache.flink.mesos.util;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract implementation of a TaskRequest.
 */
public abstract class TaskRequestBase implements TaskRequest {

	Protos.TaskID taskID;

	public TaskRequestBase(Protos.TaskID taskID) {
		this.taskID = taskID;
	}

	private final AtomicReference<AssignedResources> assignedResources = new AtomicReference<>();

	@Override
	public String getId() {
		return taskID.getValue();
	}

	@Override
	public String taskGroupName() {
		return "";
	}

	@Override
	public double getCPUs() { return 0; }

	@Override
	public double getMemory() { return 0; }

	@Override
	public double getNetworkMbps() {
		return 0.0;
	}

	@Override
	public double getDisk() {
		return 0.0;
	}

	@Override
	public int getPorts() {
		return 0;
	}

	@Override
	public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
		return Collections.emptyMap();
	}

	@Override
	public List<? extends ConstraintEvaluator> getHardConstraints() {
		return null;
	}

	@Override
	public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
		return null;
	}

	@Override
	public void setAssignedResources(AssignedResources assignedResources) {
		this.assignedResources.set(assignedResources);
	}

	@Override
	public AssignedResources getAssignedResources() {
		return assignedResources.get();
	}

	@Override
	public String toString() {
		return getClass().getName() +
			"{" +
			"cpus=" + getCPUs() +
			"memory=" + getMemory() +
			'}';

	}
}
