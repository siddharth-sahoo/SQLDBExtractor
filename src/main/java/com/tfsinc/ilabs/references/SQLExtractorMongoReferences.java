package com.tfsinc.ilabs.references;

/**
 * This class contains the various field names found in MongoDB.
 * @author siddharth.s
 */
public class SQLExtractorMongoReferences {

	public static final String FIELD_TABLE_NAME = "tableName";
	public static final String FIELD_FILTERS = "filters";
	public static final String FIELD_CONTEXTS = "contexts"; 
	public static final String FIELD_CONTEXT_COLUMN = "columns";
	public static final String FIELD_CONTEXT_NAMESPACE = "namespace"; 
	public static final String FIELD_CONTEXT_COLUMN_NAME = "name";
	public static final String FIELD_CONTEXT_COLUMN_TARGET = "target";
	public static final String FIELD_FILTER_COLUMN_NAME = "colName";
	public static final String FIELD_FILTER_OPERATOR = "operator";
	public static final String FIELD_FILTER_COLUMN_VALUE = "colValue";
	public static final String FIELD_CONTEXT_ROW_KEY = "rowKeyExpr";
	public static final String FIELD_SQL_COLUMNS = "columns";

	// Private constructor.
	private SQLExtractorMongoReferences() { }
	
}
