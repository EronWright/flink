package org.apache.flink.mesos.util;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * An allocation of resources from one or more Mesos offers, to be portioned out to tasks.
 *
 * This class assumes that the resources were offered <b>without</b> the {@code RESERVATION_REFINEMENT} capability,
 * as detailed in the "Resource Format" section of the Mesos protocol definition.
 *
 * This class is not thread-safe.
 */
public class MesosResourceAllocation {

	protected static final Logger LOG = LoggerFactory.getLogger(MesosResourceAllocation.class);

	static final double EPSILON = 1e-5;
	static final String UNRESERVED_ROLE = "*";

	private final List<Protos.Resource> resources;

	public MesosResourceAllocation(Collection<Protos.Resource> resources) {
		this.resources = new ArrayList<>(resources);

		// sort the resources to prefer reserved resources
		this.resources.sort(Comparator.comparing(r -> UNRESERVED_ROLE.equals(r.getRole())));
	}

	/**
	 * Takes some amount of scalar resources (e.g. cpus, mem).
	 *
	 * @param amount the (approximate) amount to take from the available quantity.
	 * @param roles the roles to accept
	 */
	public List<Protos.Resource> takeScalar(String resourceName, double amount, Set<String> roles) {
		LOG.debug("Allocating scalar resources - resourceName: {}, amount: {}, resources: {}", resourceName, amount, resources);

		List<Protos.Resource> result = new ArrayList<>(1);
		for (ListIterator<Protos.Resource> i = resources.listIterator(); i.hasNext();) {
			if (amount <= EPSILON) break;
			
			// take from next available scalar resource that is unreserved or reserved for an applicable role
			Protos.Resource available = i.next();
			if (!resourceName.equals(available.getName()) || !available.hasScalar()) continue;
			if (!UNRESERVED_ROLE.equals(available.getRole()) && !roles.contains(available.getRole())) continue;

			double amountToTake = Math.min(available.getScalar().getValue(), amount);
			Protos.Resource taken = available.toBuilder().setScalar(Protos.Value.Scalar.newBuilder().setValue(amountToTake)).build();
			amount -= amountToTake;
			result.add(taken);
			LOG.debug("Taking {} from {}", taken, available);

			// keep remaining amount (if any)
			double remaining = available.getScalar().getValue() - taken.getScalar().getValue();
			if (remaining > EPSILON) {
				i.set(available.toBuilder().setScalar(Protos.Value.Scalar.newBuilder().setValue(remaining)).build());
			}
			else {
				i.remove();
			}
		}

		LOG.debug("Allocated scalar resources - result: {}, unsatisfied: {}, remaining: {}", result, amount, resources);
		return result;
	}

	/**
	 * Takes some amount of range resources (e.g. ports).
	 *
	 * @param amount the number of values to take from the available range(s).
	 * @param roles the roles to accept
	 */
	public List<Protos.Resource> takeRanges(String resourceName, int amount, Set<String> roles) {
		LOG.debug("Allocating range resources - resourceName: {}, amount: {}, resources: {}", resourceName, amount, resources);

		List<Protos.Resource> result = new ArrayList<>(1);
		for (ListIterator<Protos.Resource> i = resources.listIterator(); i.hasNext();) {
			if (amount <= 0) break;

			// take from next available range resource that is unreserved or reserved for an applicable role
			Protos.Resource available = i.next();
			if (!resourceName.equals(available.getName()) || !available.hasRanges()) continue;
			if (!UNRESERVED_ROLE.equals(available.getRole()) && !roles.contains(available.getRole())) continue;

			List<Protos.Value.Range> takenRanges = new ArrayList<>();
			List<Protos.Value.Range> remainingRanges = new ArrayList<>(available.getRanges().getRangeList());
			for (ListIterator<Protos.Value.Range> j = remainingRanges.listIterator(); j.hasNext();) {
				if (amount <= 0) break;

				// take from next available range
				Protos.Value.Range availableRange = j.next();
				long amountToTake = Math.min(availableRange.getEnd() - availableRange.getBegin() + 1, amount);
				Protos.Value.Range takenRange = availableRange.toBuilder().setEnd(availableRange.getBegin() + amountToTake - 1).build();
				amount -= amountToTake;
				takenRanges.add(takenRange);

				// keep remaining range
				long remaining = availableRange.getEnd() - takenRange.getEnd();
				if (remaining > 0) {
					j.set(availableRange.toBuilder().setBegin(availableRange.getEnd() - remaining + 1).build());
				}
				else {
					j.remove();
				}
			}
			Protos.Resource taken = available.toBuilder().setRanges(Protos.Value.Ranges.newBuilder().addAllRange(takenRanges)).build();
			LOG.debug("Taking {} from {}", taken, available);
			result.add(taken);

			// keep remaining ranges (if any)
			if (remainingRanges.size() > 0) {
				i.set(available.toBuilder().setRanges(Protos.Value.Ranges.newBuilder().addAllRange(remainingRanges)).build());
			}
			else {
				i.remove();
			}
		}

		LOG.debug("Allocated range resources - result: {}, unsatisfied: {}, remaining: {}", result, amount, resources);
		return result;
	}

	@Override
	public String toString() {
		return "MesosResourceAllocation{" +
			"resources=" + resources +
			'}';
	}
}
