package org.apache.flink.runtime.security.modules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.runtime.security.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Responsible for installing a process-wide JAAS configuration.
 * <p>
 * The installed configuration combines login modules based on:
 * - the user-supplied JAAS configuration file, if any
 * - a Kerberos keytab, if configured
 * - any cached Kerberos credentials from the current environment
 * <p>
 * The module also installs a default JAAS config file (if necessary) for
 * compatibility with ZK and Kafka.  Note that the JRE actually draws on numerous file locations.
 * See: https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/login/ConfigFile.html
 * See: https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/common/security/kerberos/Login.java#L289
 */
@Internal
public class JaasModule implements SecurityModule {

	private static final Logger LOG = LoggerFactory.getLogger(JaasModule.class);

	static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

	static final String JAAS_CONF_RESOURCE_NAME = "flink-jaas.conf";

	private String priorConfigFile;
	private javax.security.auth.login.Configuration priorConfig;

	private DynamicConfiguration currentConfig;

	@Override
	public void install(SecurityUtils.SecurityConfiguration securityConfig) {

		// ensure that a config file is always defined, for compatibility with
		// ZK and Kafka which check for the system property and existence of the file
		priorConfigFile = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null);
		if (priorConfigFile == null) {
			File configFile = generateDefaultConfigFile();
			System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, configFile.getAbsolutePath());
		}

		// read the JAAS configuration file
		priorConfig = javax.security.auth.login.Configuration.getConfiguration();

		// construct a dynamic JAAS configuration
		currentConfig = new DynamicConfiguration(priorConfig);

		// wire up the configured JAAS login contexts to use the krb5 entries
		AppConfigurationEntry[] krb5Entries = getAppConfigurationEntries(securityConfig);
		for (String app : securityConfig.getLoginContextNames()) {
			currentConfig.addAppConfigurationEntry(app, krb5Entries);
		}

		javax.security.auth.login.Configuration.setConfiguration(currentConfig);
	}

	@Override
	public void uninstall() {
		System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, priorConfigFile);
		javax.security.auth.login.Configuration.setConfiguration(priorConfig);
	}

	public DynamicConfiguration getCurrentConfiguration() {
		return currentConfig;
	}

	private static AppConfigurationEntry[] getAppConfigurationEntries(SecurityUtils.SecurityConfiguration securityConfig) {

		AppConfigurationEntry userKerberosAce = null;
		if (securityConfig.useTicketCache()) {
			userKerberosAce = KerberosUtils.ticketCacheEntry();
		}
		AppConfigurationEntry keytabKerberosAce = null;
		if (securityConfig.getKeytab() != null) {
			keytabKerberosAce = KerberosUtils.keytabEntry(securityConfig.getKeytab(), securityConfig.getPrincipal());
		}

		AppConfigurationEntry[] appConfigurationEntry;
		if (userKerberosAce != null && keytabKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{keytabKerberosAce, userKerberosAce};
		} else if (keytabKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{keytabKerberosAce};
		} else if (userKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{userKerberosAce};
		} else {
			appConfigurationEntry = null;
		}

		return appConfigurationEntry;
	}

	/**
	 * Generate the default JAAS config file.
	 */
	private static File generateDefaultConfigFile() {
		// load Jaas config file to initialize SASL
		final File jaasConfFile;
		try {
			Path jaasConfPath = Files.createTempFile("jaas-", ".conf");
			try (InputStream resourceStream = JaasModule.class.getClassLoader().getResourceAsStream(JAAS_CONF_RESOURCE_NAME)) {
				Files.copy(resourceStream, jaasConfPath, StandardCopyOption.REPLACE_EXISTING);
			}
			jaasConfFile = jaasConfPath.toFile();
			jaasConfFile.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeException("unable to generate a JAAS configuration file", e);
		}
		return jaasConfFile;
	}
}
