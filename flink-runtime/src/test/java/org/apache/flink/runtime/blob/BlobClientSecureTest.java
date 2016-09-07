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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.ConfigConstants;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

import static org.junit.Assert.fail;

public class BlobClientSecureTest extends BlobClientTest {

	/**
	 * Starts the BLOB server with secure cookie enabled configuration
	 */
	@BeforeClass
	public static void startServer() {
		try {
			config.setBoolean(ConfigConstants.SECURITY_ENABLED, true);
			config.setString(ConfigConstants.SECURITY_COOKIE, "foo");
			BLOB_SERVER = new BlobServer(config);
		}
		catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test(expected = IOException.class)
	public void testInvalidSecurityCookie() throws IOException {

		BlobClient client = null;

		try {
			byte[] testBuffer = createTestBuffer();
			MessageDigest md = BlobUtils.createMessageDigest();
			md.update(testBuffer);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getPort());
			client = new BlobClient(serverAddress, null, "different");

			// Store some data
			client.put(testBuffer);
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {}
			}
		}
	}

}
