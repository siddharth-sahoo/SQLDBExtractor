package com.tfsinc.ilabs.main;

import org.apache.log4j.Logger;

import com.awesome.pro.db.mysql.AcquireJDBCConnection;
import com.awesome.pro.db.mysql.DatabaseQueryV2;
import com.awesome.pro.db.mysql.WrappedConnection;
import com.awesome.pro.executor.IThreadPool;
import com.awesome.pro.executor.ThreadPool;
import com.awesome.pro.pool.IObjectPool;
import com.awesome.pro.pool.ObjectPool;
import com.awesome.pro.utilities.PropertyFileUtility;
import com.awesome.pro.utilities.db.mongo.MongoConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.tfsinc.ilabs.references.SQLExtractorConfigReferences;

/**
 * Instantiable class which is responsible for holding the connection
 * pool and thread pool for SQL extraction process pointing to a specific
 * configuration.
 * @author siddharth.s
 */
public class SQLExtractor {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(SQLExtractor.class);

	/**
	 * Configurations to be used in extraction.
	 */
	private final PropertyFileUtility config;

	/**
	 * Thread pool executor.
	 */
	private final IThreadPool threadPool;

	/**
	 * JDBC connection pool.
	 */
	private final IObjectPool<WrappedConnection> jdbcPool;

	/**
	 * Initializes a new SQL extractor.
	 * @param configFile Name of the configuration file to read.
	 */
	SQLExtractor(final String configFile) {
		config = new PropertyFileUtility(configFile);
		threadPool = new ThreadPool(configFile);
		threadPool.start();
		jdbcPool = new ObjectPool<WrappedConnection>(configFile,
				new AcquireJDBCConnection(configFile));
		LOGGER.info("Starting SQL extraction with config file: " + configFile);
		startExtraction();
	}

	/**
	 * Waits till the thread pool has completed all executions.
	 */
	public final void waitForCompletion() {
		threadPool.waitForCompletion();
	}

	/**
	 * Starts iteration through configurations and extraction
	 * of data.
	 */
	private final void startExtraction() {
		final DBCursor cursor = MongoConnection.getDocuments(
				config.getStringValue(SQLExtractorConfigReferences.
						PROPERTY_MONGO_DATABASE_NAME),
						config.getStringValue(SQLExtractorConfigReferences.
								PROPERTY_MONGO_COLLECTION_NAME)
				);

		while (cursor.hasNext()) {
			final BasicDBObject obj = (BasicDBObject) cursor.next();
			threadPool.execute(new SQLExtractorRunnableJob(obj,
					new DatabaseQueryV2(jdbcPool),
					config));
		}

		LOGGER.info("Completed adding jobs, waiting for completion.");
	}

}
