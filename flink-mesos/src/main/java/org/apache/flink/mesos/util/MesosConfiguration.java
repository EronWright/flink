package org.apache.flink.mesos.util;


import org.apache.mesos.Protos;
import org.slf4j.Logger;
import scala.Option;

import java.util.Map;

public class MesosConfiguration {

	private String masterUrl;

	private Protos.FrameworkInfo.Builder frameworkInfo;

	private Option<Protos.Credential.Builder> credential = Option.empty();

	public MesosConfiguration(
		String masterUrl,
		Protos.FrameworkInfo.Builder frameworkInfo,
		Option<Protos.Credential.Builder> credential) {

		this.masterUrl = masterUrl;
		this.frameworkInfo = frameworkInfo;
		this.credential = credential;
	}

	public String masterUrl() {
		return masterUrl;
	}

	public Protos.FrameworkInfo.Builder frameworkInfo() {
		return frameworkInfo;
	}

	public Option<Protos.Credential.Builder> credential() {
		return credential;
	}

	@Override
	public String toString() {
		return "MesosConfiguration{" +
			"masterUrl='" + masterUrl + '\'' +
			", frameworkInfo=" + frameworkInfo +
			", credential=" + (credential.isDefined() ? "(not shown)" : "(none)") +
			'}';
	}

	/**
	 * A utility method to log relevant Mesos connection info
     */
	public static void logMesosConfig(Logger log, MesosConfiguration config) {

		Map<String,String> env = System.getenv();
		Protos.FrameworkInfo.Builder info = config.frameworkInfo();

		log.info("--------------------------------------------------------------------------------");
		log.info(" Connecting to Mesos");

		log.info(" Mesos Info:");
		log.info("    Master URL: {}", config.masterUrl());

		log.info(" Framework Info:");
		log.info("    ID: {}", info.hasId() ? info.getId().getValue() : "(none)");
		log.info("    Name: {}", info.hasName() ? info.getName() : "(none)");
		log.info("    Failover Timeout (secs): {}", info.getFailoverTimeout());
		log.info("    Role: {}", info.hasRole() ? info.getRole() : "(none)");
		log.info("    Principal: {}", info.hasPrincipal() ? info.getPrincipal() : "(none)");
		log.info("    Host: {}", info.hasHostname() ? info.getHostname() : "(none)");
		if(env.containsKey("LIBPROCESS_IP")) {
			log.info("    LIBPROCESS_IP: {}", env.get("LIBPROCESS_IP"));
		}
		if(env.containsKey("LIBPROCESS_PORT")) {
			log.info("    LIBPROCESS_PORT: {}", env.get("LIBPROCESS_PORT"));
		}
		log.info("    Web UI: {}", info.hasWebuiUrl() ? info.getWebuiUrl() : "(none)");

		log.info("--------------------------------------------------------------------------------");

	}
}
