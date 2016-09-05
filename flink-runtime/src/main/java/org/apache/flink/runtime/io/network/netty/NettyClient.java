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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;

import static org.apache.flink.util.Preconditions.checkState;

class NettyClient {

	private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

	private final NettyConfig config;

	private NettyProtocol protocol;

	private Bootstrap bootstrap;

	private SSLContext clientSSLContext;

	NettyClient(NettyConfig config) {
		this.config = config;
	}

	void init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
		checkState(bootstrap == null, "Netty client has already been initialized.");

		this.protocol = protocol;

		long start = System.currentTimeMillis();

		bootstrap = new Bootstrap();

		// --------------------------------------------------------------------
		// Transport-specific configuration
		// --------------------------------------------------------------------

		switch (config.getTransportType()) {
			case NIO:
				initNioBootstrap();
				break;

			case EPOLL:
				initEpollBootstrap();
				break;

			case AUTO:
				if (Epoll.isAvailable()) {
					initEpollBootstrap();
					LOG.info("Transport type 'auto': using EPOLL.");
				}
				else {
					initNioBootstrap();
					LOG.info("Transport type 'auto': using NIO.");
				}
		}

		// --------------------------------------------------------------------
		// Configuration
		// --------------------------------------------------------------------

		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

		// Timeout for new connections
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getClientConnectTimeoutSeconds() * 1000);

		// Pooled allocator for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

		if (config.getSSLEnabled()) {
			try {
				LOG.info("Configuring SSL for the Netty client");

				// Load the CA truststore into the client SSL Context
				KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

				FileInputStream trustStoreFile = null;
				try {
					trustStoreFile = new FileInputStream(new File(config.getSSLTrustStorePath()));
					trustStore.load(trustStoreFile, config.getSSLTrustStorePassword().toCharArray());
				} finally {
					if (trustStoreFile != null) {
						trustStoreFile.close();
					}
				}

				TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
					TrustManagerFactory.getDefaultAlgorithm());
				trustManagerFactory.init(trustStore);

				clientSSLContext = SSLContext.getInstance(config.getSSLVersion());
				clientSSLContext.init(null, trustManagerFactory.getTrustManagers(), null);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSL Context for the Netty client", e);
			}
		} else {
			clientSSLContext = null;
		}

		long end = System.currentTimeMillis();
		LOG.info("Successful initialization (took {} ms).", (end - start));
	}

	NettyConfig getConfig() {
		return config;
	}

	Bootstrap getBootstrap() {
		return bootstrap;
	}

	void shutdown() {
		long start = System.currentTimeMillis();

		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}

		long end = System.currentTimeMillis();
		LOG.info("Successful shutdown (took {} ms).", (end - start));
	}

	private void initNioBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple clients running on the same host.
		String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(nioGroup).channel(NioSocketChannel.class);
	}

	private void initEpollBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple clients running on the same host.
		String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
	}

	// ------------------------------------------------------------------------
	// Client connections
	// ------------------------------------------------------------------------

	ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
		checkState(bootstrap != null, "Client has not been initialized yet.");

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {

				// SSL handler should be added first in the pipeline
				if (clientSSLContext != null) {
					SSLEngine sslEngine = clientSSLContext.createSSLEngine(
						serverSocketAddress.getAddress().getHostAddress(),
						serverSocketAddress.getPort());
					sslEngine.setUseClientMode(true);

					// Check and enable hostname verification for the SSL connection
					if (config.getSSLVerifyHostname()) {
						SSLParameters newSslParameters = sslEngine.getSSLParameters();
						newSslParameters.setEndpointIdentificationAlgorithm("HTTPS");
						sslEngine.setSSLParameters(newSslParameters);
					}
					channel.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}
				channel.pipeline().addLast(protocol.getClientChannelHandlers());
			}
		});

		try {
			return bootstrap.connect(serverSocketAddress);
		}
		catch (io.netty.channel.ChannelException e) {
			if ( (e.getCause() instanceof java.net.SocketException &&
					e.getCause().getMessage().equals("Too many open files")) ||
				(e.getCause() instanceof io.netty.channel.ChannelException &&
						e.getCause().getCause() instanceof java.net.SocketException &&
						e.getCause().getCause().getMessage().equals("Too many open files")))
			{
				throw new io.netty.channel.ChannelException(
						"The operating system does not offer enough file handles to open the network connection. " +
								"Please increase the number of of available file handles.", e.getCause());
			}
			else {
				throw e;
			}
		}
	}
}
