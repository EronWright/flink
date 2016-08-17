package org.apache.flink.mesos.dispatcher;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.mesos.dispatcher.store.MesosSessionStore;
import org.apache.flink.mesos.dispatcher.store.StandaloneMesosSessionStore;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.mesos.Protos;
import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * Created by wrighe3 on 8/5/16.
 */
public class DispatcherUtils {

	/**
	 * Read the dispatcher's Mesos scheduler configuration from the Flink configuration.
     */
	public static MesosConfiguration createMesosConfig(Configuration flinkConfig, String hostname) {

		Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
			.setUser("")
			.setHostname(hostname);
		Protos.Credential.Builder credential = null;

		if(!flinkConfig.containsKey(ConfigConstants.MESOS_MASTER_URL)) {
			throw new IllegalConfigurationException(ConfigConstants.MESOS_MASTER_URL + " must be configured.");
		}
		String masterUrl = flinkConfig.getString(ConfigConstants.MESOS_MASTER_URL, null);

		Duration failoverTimeout = FiniteDuration.apply(
			flinkConfig.getInteger(
				ConfigConstants.MESOS_FAILOVER_TIMEOUT_SECONDS,
				ConfigConstants.DEFAULT_MESOS_FAILOVER_TIMEOUT_SECS),
			TimeUnit.SECONDS);
		frameworkInfo.setFailoverTimeout(failoverTimeout.toSeconds());

		frameworkInfo.setName(flinkConfig.getString(
			ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_NAME,
			ConfigConstants.DEFAULT_MESOS_DISPATCHER_FRAMEWORK_NAME));

		frameworkInfo.setRole(flinkConfig.getString(
			ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_ROLE,
			ConfigConstants.DEFAULT_MESOS_DISPATCHER_FRAMEWORK_ROLE));

		if(flinkConfig.containsKey(ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_PRINCIPAL)) {
			frameworkInfo.setPrincipal(flinkConfig.getString(
				ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_PRINCIPAL, null));

			credential = Protos.Credential.newBuilder();
			credential.setPrincipal(frameworkInfo.getPrincipal());

			if(!flinkConfig.containsKey(ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_SECRET)) {
				throw new IllegalConfigurationException(ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_SECRET + " must be configured.");
			}
			credential.setSecret(flinkConfig.getString(
				ConfigConstants.MESOS_DISPATCHER_FRAMEWORK_SECRET, null));
		}

		MesosConfiguration mesos =
			new MesosConfiguration(masterUrl, frameworkInfo, Option.apply(credential));

		return mesos;
	}

	public static MesosSessionStore createSessionStore(Configuration flinkConfig) {
		return new StandaloneMesosSessionStore();
	}
}
