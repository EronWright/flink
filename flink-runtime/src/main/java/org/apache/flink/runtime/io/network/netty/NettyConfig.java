/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConfig {

	private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

	// - Config keys ----------------------------------------------------------

	public static final String NUM_ARENAS = "taskmanager.net.num-arenas";

	public static final String NUM_THREADS_SERVER = "taskmanager.net.server.numThreads";

	public static final String NUM_THREADS_CLIENT = "taskmanager.net.client.numThreads";

	public static final String CONNECT_BACKLOG = "taskmanager.net.server.backlog";

	public static final String CLIENT_CONNECT_TIMEOUT_SECONDS = "taskmanager.net.client.connectTimeoutSec";

	public static final String SEND_RECEIVE_BUFFER_SIZE = "taskmanager.net.sendReceiveBufferSize";

	public static final String TRANSPORT_TYPE = "taskmanager.net.transport";

	/**
	 * Config parameter to enable SSL on the Netty transport
	 */
	public static final String SSL_ENABLED = "taskmanager.net.ssl.enable";

	/** The default value to enable ssl */
	public static final boolean DEFAULT_SSL_ENABLED = false;

	/** The SSL protocol version to be supported by the SSL connection */
	public static final String SSL_VERSION = "taskmanager.net.ssl.version";

	/** The default value for the ssl protocol */
	public static final String DEFAULT_SSL_VERSION = "TLS";

	/** The Java keystore file location which contains the certificate and key for the SSL connection */
	public static final String SSL_KEYSTORE = "taskmanager.net.ssl.keystore";

	/** The secret to decrypt the keystore file */
	public static final String SSL_KEYSTORE_PASSWORD = "taskmanager.net.ssl.keystore.password";

	/** The secret to decrypt the server key */
	public static final String SSL_KEY_PASSWORD = "taskmanager.net.ssl.key.password";

	/** The path to the Java truststore to verify the ssl certificate */
	public static final String SSL_TRUSTSTORE = "taskmanager.net.ssl.truststore";

	/** The secret to decrypt the Java truststore */
	public static final String SSL_TRUSTSTORE_PASSWORD = "taskmanager.net.ssl.truststore.password";

	/** Flag to enable/disable remote hostname verification for the ssl connection */
	public static final String SSL_VERIFY_HOSTNAME = "taskmanager.net.ssl.hostname.verify";

	/** default value for hostname verification */
	public static final boolean DEFAULT_SSL_VERIFY_HOSTNAME = true;

	// ------------------------------------------------------------------------

	enum TransportType {
		NIO, EPOLL, AUTO
	}

	final static String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

	final static String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

	private final InetAddress serverAddress;

	private final int serverPort;

	private final int memorySegmentSize;

	private final int numberOfSlots;

	private final Configuration config; // optional configuration

	public NettyConfig(
			InetAddress serverAddress,
			int serverPort,
			int memorySegmentSize,
			int numberOfSlots,
			Configuration config) {

		this.serverAddress = checkNotNull(serverAddress);

		checkArgument(serverPort > 0 && serverPort <= 65536, "Invalid port number.");
		this.serverPort = serverPort;

		checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
		this.memorySegmentSize = memorySegmentSize;

		checkArgument(numberOfSlots > 0, "Number of slots");
		this.numberOfSlots = numberOfSlots;

		this.config = checkNotNull(config);

		LOG.info(this.toString());
	}

	InetAddress getServerAddress() {
		return serverAddress;
	}

	int getServerPort() {
		return serverPort;
	}

	int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	// ------------------------------------------------------------------------
	// Setters
	// ------------------------------------------------------------------------

	public NettyConfig setServerConnectBacklog(int connectBacklog) {
		checkArgument(connectBacklog >= 0);
		config.setInteger(CONNECT_BACKLOG, connectBacklog);

		return this;
	}

	public NettyConfig setServerNumThreads(int numThreads) {
		checkArgument(numThreads >= 0);
		config.setInteger(NUM_THREADS_SERVER, numThreads);

		return this;
	}

	public NettyConfig setClientNumThreads(int numThreads) {
		checkArgument(numThreads >= 0);
		config.setInteger(NUM_THREADS_CLIENT, numThreads);

		return this;
	}

	public NettyConfig setClientConnectTimeoutSeconds(int connectTimeoutSeconds) {
		checkArgument(connectTimeoutSeconds >= 0);
		config.setInteger(CLIENT_CONNECT_TIMEOUT_SECONDS, connectTimeoutSeconds);

		return this;
	}

	public NettyConfig setSendAndReceiveBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0);
		config.setInteger(SEND_RECEIVE_BUFFER_SIZE, bufferSize);

		return this;
	}

	public NettyConfig setTransportType(String transport) {
		if (transport.equals("nio") || transport.equals("epoll") || transport.equals("auto")) {
			config.setString(TRANSPORT_TYPE, transport);
		}
		else {
			throw new IllegalArgumentException("Unknown transport type.");
		}

		return this;
	}

	// ------------------------------------------------------------------------
	// Getters
	// ------------------------------------------------------------------------

	public int getServerConnectBacklog() {
		// default: 0 => Netty's default
		return config.getInteger(CONNECT_BACKLOG, 0);
	}

	public int getNumberOfArenas() {
		// default: number of slots
		return config.getInteger(NUM_ARENAS, numberOfSlots);
	}

	public int getServerNumThreads() {
		// default: number of task slots
		return config.getInteger(NUM_THREADS_SERVER, numberOfSlots);
	}

	public int getClientNumThreads() {
		// default: number of task slots
		return config.getInteger(NUM_THREADS_CLIENT, numberOfSlots);
	}

	public int getClientConnectTimeoutSeconds() {
		// default: 120s = 2min
		return config.getInteger(CLIENT_CONNECT_TIMEOUT_SECONDS, 120);
	}

	public int getSendAndReceiveBufferSize() {
		// default: 0 => Netty's default
		return config.getInteger(SEND_RECEIVE_BUFFER_SIZE, 0);
	}

	public TransportType getTransportType() {
		String transport = config.getString(TRANSPORT_TYPE, "nio");

		if (transport.equals("nio")) {
			return TransportType.NIO;
		}
		else if (transport.equals("epoll")) {
			return TransportType.EPOLL;
		}
		else {
			return TransportType.AUTO;
		}
	}

	public boolean getSSLEnabled() {
		return config.getBoolean(SSL_ENABLED, DEFAULT_SSL_ENABLED);
	}

	public String getSSLVersion() {
		return config.getString(SSL_VERSION, DEFAULT_SSL_VERSION);
	}

	public String getSSLKeyStorePath() {
		return config.getString(SSL_KEYSTORE, null);
	}

	public String getSSLKeyStorePassword() {
		return config.getString(SSL_KEYSTORE_PASSWORD, null);
	}

	public String getSSLKeyPassword() {
		return config.getString(SSL_KEY_PASSWORD, null);
	}

	public String getSSLTrustStorePath() {
		return config.getString(SSL_TRUSTSTORE, null);
	}

	public String getSSLTrustStorePassword() {
		return config.getString(SSL_TRUSTSTORE_PASSWORD, null);
	}

	public boolean getSSLVerifyHostname() {
		return config.getBoolean(SSL_VERIFY_HOSTNAME, DEFAULT_SSL_VERIFY_HOSTNAME);
	}

	@Override
	public String toString() {
		String format = "NettyConfig [" +
				"server address: %s, " +
				"server port: %d, " +
				"ssl enabled: %s, " +
				"memory segment size (bytes): %d, " +
				"transport type: %s, " +
				"number of server threads: %d (%s), " +
				"number of client threads: %d (%s), " +
				"server connect backlog: %d (%s), " +
				"client connect timeout (sec): %d, " +
				"send/receive buffer size (bytes): %d (%s)]";

		String def = "use Netty's default";
		String man = "manual";

		return String.format(format, serverAddress, serverPort, getSSLEnabled() ? "true":"false",
				memorySegmentSize, getTransportType(), getServerNumThreads(),
				getServerNumThreads() == 0 ? def : man,
				getClientNumThreads(), getClientNumThreads() == 0 ? def : man,
				getServerConnectBacklog(), getServerConnectBacklog() == 0 ? def : man,
				getClientConnectTimeoutSeconds(), getSendAndReceiveBufferSize(),
				getSendAndReceiveBufferSize() == 0 ? def : man);
	}
}
