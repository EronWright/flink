---
title: "SSL Setup"
nav-parent_id: setup
nav-pos: 8
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This page provides instructions on how to enable SSL for the network communication between different flink components.

## SSL Configuration

SSL can be enabled for all network communication between flink components. SSL keystores and truststore has to be deployed on each flink node and configured (conf/flink-conf.yaml) using keys in the security.ssl.* namespace (Please see the [configuration page](config.html) for details). SSL can be selectively enabled/disabled for different transports using the following flags. These flags are only applicable when security.ssl.enabled is set to true.

* **taskmanager.data.ssl.enabled**: SSL flag for data communication between task managers
* **blob.service.ssl.enabled**: SSL flag for BLOB service client/server communication
* **akka.ssl.enabled**: SSL flag for the akka based control connection between the flink client, jobmanager and taskmanager 
* **jobmanager.web.ssl.enabled**: Flag to enable https access to the jobmanager's web frontend

## Deploying Keystores and truststores

You need to have a Java Keystore generated and copied to each node in the flink cluster. The common name or subject alternative names in the certificate should match the node's hostname and IP address. Keystores and truststores can be generated using the keytool utility (https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html). All flink components should have read access to the keystore and truststore files.

## Standalone Deployment
Configure each node in the standalone cluster to pick up the keystore and truststore files present in the local file system.

### Example: 2 node cluster
* Generate 2 keystores, one for each node, and copy them to the filesystem on the respective node. Also copy the pulic key of the CA (which was used to sign the certificates in the keystore) as a Java truststore on both the nodes
* Configure conf/flink-conf.yaml to pick up these files

#### Node1:
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node1.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/common-ca.truststore
security.ssl.truststore-password: abc123
~~~

#### Node2:
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node2.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/common-ca.truststore
security.ssl.truststore-password: abc123
~~~

* Restart the flink components to enable SSL for all of flink's internal communication
* Verify by accessing the jobmanager's UI using https url. The task manager's path in the UI should show akka.ssl.tcp:// as the protocol
* The blob server and task manager's data communication can be verified from the log files

## YARN Deployment
The keystores and truststore should be generated and deployed on all nodes in the YARN setup where flink components can potentially be executed. The same flink config file from the flink YARN client is used for all the flink components running in the YARN cluster. Therefore we need to ensure the keystore is deployed and accessible using the same filepath in all the YARN nodes.

### Example config:
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/common-ca.truststore
security.ssl.truststore-password: abc123
~~~

