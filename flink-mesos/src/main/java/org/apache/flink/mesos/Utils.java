package org.apache.flink.mesos;

import org.apache.mesos.Protos;

import java.net.URL;

public class Utils {
	/**
	 * Construct a Mesos environment variable.
     */
	public static Protos.Environment.Variable variable(String name, String value) {
		return Protos.Environment.Variable.newBuilder()
			.setName(name)
			.setValue(value)
			.build();
	}

	/**
	 * Construct a Mesos URI.
     */
	public static Protos.CommandInfo.URI uri(URL url, boolean cacheable) {
		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.toExternalForm())
			.setExtract(false)
			.setCache(cacheable)
			.build();
	}
}
