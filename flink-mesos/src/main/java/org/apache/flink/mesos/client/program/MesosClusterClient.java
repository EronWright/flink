package org.apache.flink.mesos.client.program;

import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * The Mesos cluster client.
 *
 * WIP
 */
public class MesosClusterClient extends StandaloneClusterClient {

	public MesosClusterClient(Configuration flinkConfig) throws IOException {
		super(flinkConfig);
	}
}
