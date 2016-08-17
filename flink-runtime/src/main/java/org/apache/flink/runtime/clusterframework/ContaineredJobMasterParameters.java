package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class describes the basic parameters for launching a JobMaster process.
 */
public class ContaineredJobMasterParameters implements java.io.Serializable {

	/** Total container memory, in bytes */
	private final long totalContainerMemoryMB;

	/** Heap size to be used for the Java process */
	private final long jobMasterHeapSizeMB;

	/** Environment variables to add to the Java process */
	private final HashMap<String, String> jobMasterEnv;

	public ContaineredJobMasterParameters(long totalContainerMemoryMB, long jobMasterHeapSizeMB, HashMap<String, String> jobMasterEnv) {
		this.totalContainerMemoryMB = totalContainerMemoryMB;
		this.jobMasterHeapSizeMB = jobMasterHeapSizeMB;
		this.jobMasterEnv = jobMasterEnv;
	}

	public long totalContainerMemoryMB() {
		return totalContainerMemoryMB;
	}

	public long jobMasterHeapSizeMB() {
		return jobMasterHeapSizeMB;
	}

	public Map<String, String> jobMasterEnv() {
		return Collections.unmodifiableMap(jobMasterEnv);
	}

	/**
	 * Computes the parameters to be used to start a JobMaster Java process.
	 *
	 * @param config The Flink configuration.
	 * @param containerMemoryMB The size of the complete container, in megabytes.
	 * @return The parameters to start the JobMaster processes with.
	 */
	public static ContaineredJobMasterParameters create(
		Configuration config, long containerMemoryMB)
	{
		// (1) compute how much memory we subtract from the total memory, to get the Java memory

		final float memoryCutoffRatio = config.getFloat(
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_RATIO,
			ConfigConstants.DEFAULT_YARN_HEAP_CUTOFF_RATIO);

		final int minCutoff = config.getInteger(
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_MIN,
			ConfigConstants.DEFAULT_YARN_HEAP_CUTOFF);

		if (memoryCutoffRatio >= 1 || memoryCutoffRatio <= 0) {
			throw new IllegalArgumentException("The configuration value '"
				+ ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_RATIO + "' must be between 0 and 1. Value given="
				+ memoryCutoffRatio);
		}

		if (minCutoff >= containerMemoryMB) {
			throw new IllegalArgumentException("The configuration value '"
				+ ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_MIN + "'='" + minCutoff
				+ "' is larger than the total container memory " + containerMemoryMB);
		}

		long cutoff = (long) (containerMemoryMB * memoryCutoffRatio);
		if (cutoff < minCutoff) {
			cutoff = minCutoff;
		}

		final long heapSizeMB = containerMemoryMB - cutoff;

		// (2) obtain the additional environment variables from the configuration
		final HashMap<String, String> envVars = new HashMap<>();
		final String prefix = ConfigConstants.CONTAINERIZED_MASTER_ENV_PREFIX;

		for (String key : config.keySet()) {
			if (key.startsWith(prefix) && key.length() > prefix.length()) {
				// remove prefix
				String envVarKey = key.substring(prefix.length());
				envVars.put(envVarKey, config.getString(key, null));
			}
		}

		// done
		return new ContaineredJobMasterParameters(
			containerMemoryMB, heapSizeMB, envVars);
	}
}
