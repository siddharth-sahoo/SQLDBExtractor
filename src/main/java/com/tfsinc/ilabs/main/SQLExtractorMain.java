package com.tfsinc.ilabs.main;

import gnu.trove.map.hash.THashMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * Main class for extracting data from staging DB.
 * @author siddharth.s
 */
public class SQLExtractorMain {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			SQLExtractorMain.class);

	/**
	 * Map of extractor name to extractor instance.
	 */
	private static final Map<String, SQLExtractor> INSTANCES = new THashMap<>();

	/**
	 * Starts the extraction process from a SQL database.
	 * @param name Name to reference the extractor instance.
	 * @param configFile Name of the configuration file to read.
	 */
	public static final void start(final String name, final String configFile) {
		if (INSTANCES.containsKey(name)) {
			synchronized (SQLExtractorMain.class) {
				if (!INSTANCES.containsKey(name)) {
					INSTANCES.put(name, new SQLExtractor(configFile));
				}
			}
		}
		LOGGER.warn("Extractor instance already present. Ignoring.");
	}

	/**
	 * Wait for completion of all instances of SQL extractors.
	 */
	public static final void waitForCompletion() {
		final Iterator<Entry<String, SQLExtractor>> iter =
				INSTANCES.entrySet().iterator();
		while (iter.hasNext()) {
			final Entry<String, SQLExtractor> entry = iter.next();
			LOGGER.info("Waiting for completion of SQL extractor: "
					+ entry.getKey());
			entry.getValue().waitForCompletion();
		}
	}

	/**
	 * Serves as a single point of exit or failure.
	 * @param reason Reason for failure.
	 */
	public static final void fail(final String reason) {
		LOGGER.error("Exiting: " + reason);
		System.exit(1);
	}

}
