package com.tfsinc.ilabs.main;

import gnu.trove.map.hash.THashMap;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.mvel2.MVEL;

import com.awesome.pro.context.ContextBuilder;
import com.awesome.pro.context.ContextFactory;
import com.awesome.pro.db.mysql.IDatabaseQuery;
import com.awesome.pro.utilities.PropertyFileUtility;
import com.awesome.pro.utilities.StringUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.tfsinc.ilabs.references.SQLExtractorConfigReferences;
import com.tfsinc.ilabs.references.SQLExtractorMongoReferences;
import com.tfsinc.ilabs.references.SQLExtractorQueryReferences;

/**
 * DBExtractorRunnableJob Class creates SQL statements and executes them in
 * parallel
 * 
 * @author adithya.krishna
 */
class SQLExtractorRunnableJob implements Runnable {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			SQLExtractorRunnableJob.class);

	/**
	 * MongoDB mappings for single extraction step.
	 */
	private final BasicDBObject mapping;

	/**
	 * Database query utility.
	 */
	private final IDatabaseQuery dbQuery;

	/**
	 * Property file to be read for additional filtering.
	 */
	private final PropertyFileUtility properties;

	/**
	 * Initializes a runnable job which operates on a single table.
	 * @param config Mappings retrieved from MongoDB.
	 * @param dbq Database query utility.
	 * @param conf Property file to read for additional filters.
	 */
	public SQLExtractorRunnableJob(final BasicDBObject config,
			final IDatabaseQuery dbq, PropertyFileUtility conf) {
		mapping = config;
		dbQuery = dbq;
		properties = conf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		final String query = constructQuery();
		buildContext(query);
		try {
			dbQuery.close();
		} catch (Exception e) {
			LOGGER.error("Unable to return JDBC connection.", e);
			SQLExtractorMain.fail("Unable to return JDBC connection.");
		}
	}

	/**
	 * @return Constructed query for the current table.
	 */
	private final String constructQuery() {
		final BasicDBList columns = (BasicDBList) mapping.get(
				SQLExtractorMongoReferences.FIELD_SQL_COLUMNS);
		final int columnCount = columns.size();
		String query = SQLExtractorQueryReferences.SELECT;

		for (int i = 0; i < columnCount; i ++) {
			query = query + columns.get(i)
					+ SQLExtractorQueryReferences.ENTITY_SEPARATOR;
		}
		query = query.substring(0, query.length() - 2)
				+ SQLExtractorQueryReferences.FROM
				+ mapping.getString(SQLExtractorMongoReferences.FIELD_TABLE_NAME);

		final BasicDBList filters = (BasicDBList) mapping.get(
				SQLExtractorMongoReferences.FIELD_FILTERS);
		final int filterCount = filters.size();

		if (filterCount > 0) {
			query = query + SQLExtractorQueryReferences.WHERE;

			for (int i = 0; i < filterCount; i ++) {
				final BasicDBObject filter = (BasicDBObject) filters.get(i);

				query = query 
						+ filter.getString(
								SQLExtractorMongoReferences.FIELD_FILTER_COLUMN_NAME)
								+ SQLExtractorQueryReferences.SPACE
								+ filter.getString(SQLExtractorMongoReferences.FIELD_FILTER_OPERATOR)
								+ SQLExtractorQueryReferences.SPACE
								+ properties.getStringValue(
										filter.getString(SQLExtractorMongoReferences.FIELD_FILTER_COLUMN_VALUE))
										+ SQLExtractorQueryReferences.AND;
			}

			query = StringUtils.substringBeforeLast(query, SQLExtractorQueryReferences.AND);
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing query: " + query);
		}

		return query;
	}

	private final void buildContext(final String query) {
		final CachedRowSet crs = dbQuery.runQuery(query);

		final BasicDBList contexts = (BasicDBList) mapping.get(
				SQLExtractorMongoReferences.FIELD_CONTEXTS);
		final int contextCount = contexts.size();

		final ContextBuilder contextBuilder = ContextFactory.getContextBuilder(
				properties.getStringValue(
						SQLExtractorConfigReferences.PROPERTY_CONTEXT_NAME));

		try {
			while (crs.next()) {
				for (int i = 0; i < contextCount; i ++) {
					final BasicDBObject context = (BasicDBObject) contexts.get(i);

					final String namespace = context.getString(
							SQLExtractorMongoReferences.FIELD_CONTEXT_NAMESPACE);

					final Map<String, String> sqlValues = getSQLValues(crs,
							(BasicDBList) mapping.get(
									SQLExtractorMongoReferences.FIELD_SQL_COLUMNS));
					final String rowKey = MVEL.evalToString(
							context.getString(
									SQLExtractorMongoReferences.FIELD_CONTEXT_ROW_KEY),
									sqlValues);
					final Map<String, String> contextData = getContextData(
							(BasicDBList) context.get(
									SQLExtractorMongoReferences.FIELD_CONTEXT_COLUMN),
									sqlValues);

					contextBuilder.addContextNameSpace(namespace);
					contextBuilder.addContextualData(namespace, rowKey, contextData);
				}
			}
		} catch (SQLException e) {
			LOGGER.error("Unable to iterate through row set.", e);
			SQLExtractorMain.fail("Unable to iterate through row set.");
		}
	}

	/**
	 * @param colummMappings Configurations mapping SQL column names to Cassandra
	 * column names.
	 * @param sqlValues Map of SQL column name to value for a particular row.
	 * @return Map of Cassandra column name to value derived from SQL values.
	 */
	private Map<String, String> getContextData(final BasicDBList colummMappings,
			final Map<String, String> sqlValues) {
		final Map<String, String> contextData = new THashMap<>();
		final int size = colummMappings.size();

		for (int i = 0; i < size; i ++) {
			final BasicDBObject mapping = (BasicDBObject) colummMappings.get(i);
			contextData.put(
					mapping.getString(
							SQLExtractorMongoReferences
							.FIELD_CONTEXT_COLUMN_TARGET),
							sqlValues.get(
									mapping.getString(
											SQLExtractorMongoReferences
											.FIELD_CONTEXT_COLUMN_NAME)
									)
					);
		}

		return contextData;
	}

	/**
	 * @param crs SQL data source.
	 * @param columns List of column names.
	 * @return Map of column name to column values.
	 */
	private static final Map<String, String> getSQLValues(final CachedRowSet crs,
			final BasicDBList columns) {
		final int size = columns.size();
		final Map<String, String> map = new THashMap<>();

		for (int i = 0; i < size; i ++) {
			final String columnName = columns.get(i).toString();
			try {
				map.put(columnName, crs.getString(columnName));
			} catch (SQLException e) {
				LOGGER.error("Unable to fetch column value.", e);
				SQLExtractorMain.fail("Unable to fetch column value.");
			}
		}

		return map;
	}

}
