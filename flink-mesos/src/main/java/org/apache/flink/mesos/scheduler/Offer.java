package org.apache.flink.mesos.scheduler;

import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An adapter class to transform a Mesos resource offer to a Fenzo {@link VirtualMachineLease}.
 *
 */
public class Offer implements VirtualMachineLease {

	private static final Logger logger = LoggerFactory.getLogger(Offer.class);

	private final Protos.Offer offer;
	private final String hostname;
	private final String vmID;
	private final long offeredTime;

	private final List<Protos.Resource> resources;
	private final Map<String, List<Protos.Resource>> resourceMap;
	private final Map<String, Protos.Attribute> attributeMap;

	public Offer(Protos.Offer offer) {
		this.offer = offer;
		this.hostname = offer.getHostname();
		this.vmID = offer.getSlaveId().getValue();
		this.offeredTime = System.currentTimeMillis();

		this.resources = new ArrayList<>(offer.getResourcesList().size());
		this.resourceMap = new HashMap<>();
		for (Protos.Resource resource : offer.getResourcesList()) {
			switch (resource.getType()) {
				case SCALAR:
				case RANGES:
					resources.add(resource);
					resourceMap.computeIfAbsent(resource.getName(), k -> new ArrayList<>(2)).add(resource);
					break;
				default:
					logger.debug("Unknown resource type " + resource.getType() + " for resource " + resource.getName() +
						" in offer, hostname=" + hostname + ", offerId=" + offer.getId());
			}
		}

		if(offer.getAttributesCount()>0) {
			Map<String, Protos.Attribute> attributeMap = new HashMap<>();
			for(Protos.Attribute attribute: offer.getAttributesList()) {
				attributeMap.put(attribute.getName(), attribute);
			}
			this.attributeMap = Collections.unmodifiableMap(attributeMap);
		} else {
			this.attributeMap = Collections.emptyMap();
		}
	}

	public List<Protos.Resource> getResources() {
		return Collections.unmodifiableList(resources);
	}

	@Override
	public String hostname() {
		return hostname;
	}

	@Override
	public String getVMID() {
		return vmID;
	}

	@Override
	public double cpuCores() {
		return aggregateScalarResource("cpus");
	}

	public List<Protos.Resource> cpuResources() {
		return resourceMap.getOrDefault("cpus", Collections.emptyList());
	}

	@Override
	public double memoryMB() {
		return aggregateScalarResource("mem");
	}

	public List<Protos.Resource> memoryResources() {
		return resourceMap.getOrDefault("mem", Collections.emptyList());
	}

	@Override
	public double networkMbps() {
		return aggregateScalarResource("network");
	}

	@Override
	public double diskMB() {
		return aggregateScalarResource("disk");
	}

	public Protos.Offer getOffer(){
		return offer;
	}

	@Override
	public String getId() {
		return offer.getId().getValue();
	}

	@Override
	public long getOfferedTime() {
		return offeredTime;
	}

	@Override
	public List<Range> portRanges() {
		return aggregateRangesResource("ports");
	}

	@Override
	public Map<String, Protos.Attribute> getAttributeMap() {
		return attributeMap;
	}

	@Override
	public String toString() {
		return "Offer{" +
			"offer=" + offer +
			", resources=" + resources.toString() +
			", hostname='" + hostname + '\'' +
			", vmID='" + vmID + '\'' +
			", attributeMap=" + attributeMap +
			", offeredTime=" + offeredTime +
			'}';
	}

	private double aggregateScalarResource(String resourceName) {
		if (resourceMap.get(resourceName) == null) {
			return 0.0;
		}
		return resourceMap.get(resourceName).stream().mapToDouble(r -> r.getScalar().getValue()).sum();
	}

	private List<Range> aggregateRangesResource(String resourceName) {
		if (resourceMap.get(resourceName) == null) {
			return Collections.emptyList();
		}
		return resourceMap.get(resourceName).stream()
			.flatMap(r -> r.getRanges().getRangeList().stream())
			.map(r -> new Range((int)r.getBegin(), (int) r.getEnd()))
			.collect(Collectors.toList());
	}
}
