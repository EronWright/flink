package org.apache.flink.client.deployment;

import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builds session artifacts.
 */
public class SessionArtifactHelper {

	private static final Logger LOG = LoggerFactory.getLogger(SessionArtifactHelper.class);

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";


	private final List<ShipFile> shipFiles = new LinkedList<>();

	private boolean hasLogback = false;
	private boolean hasLog4j = false;

	public SessionArtifactHelper() {

	}

	public boolean hasLogback() {
		return hasLogback;
	}

	public boolean hasLog4j() {
		return hasLog4j;
	}

	public SessionArtifactHelper addAll(ShipFile... files) {
		shipFiles.addAll(Arrays.asList(files));
		return this;
	}

	/**
	 * Add configuration files to the session artifacts.
	 *
	 * @param configDir the configuration directory.
	 */
	public SessionArtifactHelper setConfigurationDirectory(File configDir) {
		checkArgument(configDir.isDirectory(), "configDir must be a existing directory");

		// check if there is a logback or log4j file
		File logbackFile = new File(configDir, CONFIG_FILE_LOGBACK_NAME);
		hasLogback = logbackFile.exists();
		if (hasLogback) {
			shipFiles.add(new ShipFile(logbackFile, new Path(CONFIG_FILE_LOGBACK_NAME)));
		}

		File log4jFile = new File(configDir, CONFIG_FILE_LOG4J_NAME);
		hasLog4j = log4jFile.exists();
		if (hasLog4j) {
			shipFiles.add(new ShipFile(log4jFile, new Path(CONFIG_FILE_LOG4J_NAME)));
			if (hasLogback) {
				// this means there is already a logback configuration file --> fail
				LOG.warn("The configuration directory ('" + configDir + "') contains both LOG4J and " +
					"Logback configuration files. Please delete or rename one of them.");
			}
		}

		return this;
	}

	/**
	 * Add library file to the session artifacts.
	 *
	 * @param libDir the lib directory.
	 */
	public SessionArtifactHelper setLibDirectory(File libDir) {
		checkArgument(libDir.isDirectory(), "libDir must be an existing directory");

		shipFiles.add(new ShipFile(libDir, new Path(".")));

		return this;
	}

	public List<ShipFile> build() {
		return shipFiles;
	}

	public static List<ShipFile> expand(List<ShipFile> shipFiles) {

		// expand directories into files, and apply exclusions

		Queue<ShipFile> queue = new LinkedList<>(shipFiles);
		List<ShipFile> result = new LinkedList<>();

		while (!queue.isEmpty()) {
			ShipFile file = queue.remove();
			if (file.getLocalPath().isDirectory()) {
				for (File child : file.getLocalPath().listFiles()) {
					ShipFile childFile =
						new ShipFile(child, new Path(file.getRemotePath(), child.getName()));
					if (child.isDirectory()) {
						queue.add(childFile);
					} else {
						if (!excludeShipFile(childFile.getLocalPath())) {
							result.add(childFile);
						}
					}
				}
			} else {
				if (!excludeShipFile(file.getLocalPath())) {
					result.add(file);
				}
			}
		}

		return result;
	}

	private static boolean excludeShipFile(File shipFile) {
		// remove uberjar from ship list (by default everything in the lib/ folder is added to
		// the list of files to ship, but we handle the uberjar separately.
		return shipFile.getName().startsWith("flink-dist") && shipFile.getName().endsWith("jar");
	}

	/**
	 * Represents a file or directory to ship as a session artifact.
	 */
	public static class ShipFile implements Serializable {
		private static final long serialVersionUID = 1L;

		private File localPath;
		private Path remotePath;

		public ShipFile(File localPath, Path remotePath) {
			this.localPath = checkNotNull(localPath);
			this.remotePath = checkNotNull(remotePath);
			checkArgument(!remotePath.isAbsolute(), "remotePath must be relative");
		}

		public File getLocalPath() {
			return localPath;
		}

		public Path getRemotePath() {
			return remotePath;
		}

		@Override
		public String toString() {
			return "ShipFile{" +
				"localPath=" + localPath +
				", remotePath=" + remotePath +
				'}';
		}
	}
}
