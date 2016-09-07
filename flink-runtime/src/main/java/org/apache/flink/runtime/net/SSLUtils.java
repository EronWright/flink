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

package org.apache.flink.runtime.net;


import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Common utilities to manage SSL transport configuration
 */
public class SSLUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

	public static boolean getSSLEnabled(Configuration sslConfig) {

		Preconditions.checkNotNull(sslConfig);

		return sslConfig.getBoolean( ConfigConstants.SECURITY_SSL_ENABLED,
			ConfigConstants.DEFAULT_SECURITY_SSL_ENABLED);
	}

	public static boolean getSSLVerifyHostname(Configuration sslConfig) {

		Preconditions.checkNotNull(sslConfig);

		return sslConfig.getBoolean(ConfigConstants.SECURITY_SSL_VERIFY_HOSTNAME,
			ConfigConstants.DEFAULT_SECURITY_SSL_VERIFY_HOSTNAME);
	}

	public static SSLContext createSSLClientContext(Configuration sslConfig) throws Exception {

		Preconditions.checkNotNull(sslConfig);
		SSLContext clientSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating client SSL context from configuration");

			String trustStoreFilePath = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_TRUSTSTORE,
				null);
			String trustStorePassword = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD,
				null);
			String sslProtocolVersion = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_PROTOCOL,
				ConfigConstants.DEFAULT_SECURITY_SSL_PROTOCOL);

			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

			FileInputStream trustStoreFile = null;
			try {
				trustStoreFile = new FileInputStream(new File(trustStoreFilePath));
				trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
			} finally {
				if (trustStoreFile != null) {
					trustStoreFile.close();
				}
			}

			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
				TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);

			clientSSLContext = SSLContext.getInstance(sslProtocolVersion);
			clientSSLContext.init(null, trustManagerFactory.getTrustManagers(), null);
		}

		return clientSSLContext;
	}

	public static SSLContext createSSLServerContext(Configuration sslConfig) throws Exception {

		Preconditions.checkNotNull(sslConfig);
		SSLContext serverSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating server SSL context from configuration");

			String keystoreFilePath = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_KEYSTORE,
				null);

			String keystorePassword = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD,
				null);

			String certPassword = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_KEY_PASSWORD,
				null);

			String sslProtocolVersion = sslConfig.getString(
				ConfigConstants.SECURITY_SSL_PROTOCOL,
				ConfigConstants.DEFAULT_SECURITY_SSL_PROTOCOL);

			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
			FileInputStream keyStoreFile = null;
			try {
				keyStoreFile = new FileInputStream(new File(keystoreFilePath));
				ks.load(keyStoreFile, keystorePassword.toCharArray());
			} finally {
				if (keyStoreFile != null) {
					keyStoreFile.close();
				}
			}

			// Set up key manager factory to use the server key store
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(
					KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, certPassword.toCharArray());

			// Initialize the SSLContext
			serverSSLContext = SSLContext.getInstance(sslProtocolVersion);
			serverSSLContext.init(kmf.getKeyManagers(), null, null);
		}

		return serverSSLContext;
	}

}
