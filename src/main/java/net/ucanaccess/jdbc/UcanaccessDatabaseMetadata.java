/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.
identifier
This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

 */
package net.ucanaccess.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.hsqldb.jdbc.JDBCDatabaseMetaData;

import net.ucanaccess.converters.SQLConverter;

public class UcanaccessDatabaseMetadata implements DatabaseMetaData {
	private class MetadataResultSet extends UcanaccessResultSet {
		public MetadataResultSet(ResultSet wrapped) {
			super(wrapped, null);
		}

		public Object getObject(int idx) throws SQLException {
			Object obj = super.getObject(idx);
			if ((idx == 1 || idx == 2) && "PUBLIC".equals(obj)) {
				return null;
			} else
				return obj;
		}

		public String getString(int idx) throws SQLException {
			Object obj= this.getObject(idx);
			if (obj==null)return null;
			else if(obj instanceof String)return (String)obj;
			return obj.toString();
			
		}
	}

	private UcanaccessConnection connection;
	private DatabaseMetaData wrapped;

	public UcanaccessDatabaseMetadata(DatabaseMetaData wrapped,
			UcanaccessConnection connection) {
		this.wrapped = wrapped;
		this.connection = connection;
	}

	public boolean allProceduresAreCallable() throws SQLException {
		try {
			return wrapped.allProceduresAreCallable();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean allTablesAreSelectable() throws SQLException {
		try {
			return wrapped.allTablesAreSelectable();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		try {
			return wrapped.autoCommitFailureClosesAllResultSets();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		try {
			return wrapped.dataDefinitionCausesTransactionCommit();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		try {
			return wrapped.dataDefinitionIgnoredInTransactions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean deletesAreDetected(int type) throws SQLException {
		try {
			return wrapped.deletesAreDetected(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		try {
			return wrapped.doesMaxRowSizeIncludeBlobs();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean generatedKeyAlwaysReturned() throws SQLException {
			return false;
	}

	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		try {
			return wrapped.getAttributes(catalog, schemaPattern,
					typeNamePattern, attributeNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		try {
			table = SQLConverter.escapeIdentifier(table).toUpperCase();
			return wrapped.getBestRowIdentifier(catalog, schema, table, scope,
					nullable);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getCatalogs() throws SQLException {
		if (this.connection.isShowSchema()) {
			try {
				return wrapped.getCatalogs();
			} catch (SQLException e) {
				throw new UcanaccessSQLException(e);
			}
		}
		throw new FeatureNotSupportedException();
	}

	public String getCatalogSeparator() throws SQLException {
		try {
			return wrapped.getCatalogSeparator();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getCatalogTerm() throws SQLException {
		try {
			return wrapped.getCatalogTerm();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getClientInfoProperties() throws SQLException {
		try {
			return wrapped.getClientInfoProperties();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		try {
			columnNamePattern = normalizeName(columnNamePattern);
			table = normalizeName(table);
			return new MetadataResultSet(wrapped.getColumnPrivileges("PUBLIC",
					"PUBLIC", table, columnNamePattern));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		try {
			columnNamePattern = normalizeName(columnNamePattern);
			tableNamePattern = normalizeName(tableNamePattern);
			return new MetadataResultSet(wrapped.getColumns("PUBLIC", "PUBLIC",
					tableNamePattern, columnNamePattern));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		try {
			parentTable = normalizeName(parentTable);
			foreignTable = normalizeName(foreignTable);
			return wrapped.getCrossReference(parentCatalog, parentSchema,
					parentTable, foreignCatalog, foreignSchema, foreignTable);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getDatabaseMajorVersion() throws SQLException {
		try {
			return wrapped.getDatabaseMajorVersion();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getDatabaseMinorVersion() throws SQLException {
		try {
			return wrapped.getDatabaseMinorVersion();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getDatabaseProductName() throws SQLException {

		return "Ucanaccess for access db(Jet) using hasqldb";

	}

	public String getDatabaseProductVersion() throws SQLException {
		try {
			return this.connection.getDbIO().getFileFormat().toString();
		} catch (IOException e) {
			return "";
		}
	}

	public int getDefaultTransactionIsolation() throws SQLException {
		try {
			return wrapped.getDefaultTransactionIsolation();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getDriverMajorVersion() {
		return 0;
	}

	public int getDriverMinorVersion() {
		return 0;
	}

	public String getDriverName() throws SQLException {
		return "Ucanaccess";
	}

	public String getDriverVersion() throws SQLException {
		return "2.0.0";
	}

	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		try {
			table = normalizeName(table);
			return new MetadataResultSet(wrapped.getExportedKeys("PUBLIC",
					"PUBLIC", table));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getExtraNameCharacters() throws SQLException {
		try {
			return wrapped.getExtraNameCharacters();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		try {
			columnNamePattern = normalizeName(columnNamePattern);
			return wrapped.getFunctionColumns(catalog, schemaPattern,
					functionNamePattern, columnNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		try {
			return wrapped.getFunctions(catalog, schemaPattern,
					functionNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getIdentifierQuoteString() throws SQLException {
		return "`";
	}

	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		try {
			table = normalizeName(table);
			return new MetadataResultSet(wrapped.getImportedKeys("PUBLIC",
					"PUBLIC", table));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		try {
			table = normalizeName(table);
			return new MetadataResultSet(wrapped.getIndexInfo("PUBLIC",
					"PUBLIC", table, unique, approximate));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getJDBCMajorVersion() throws SQLException {
		try {
			return wrapped.getJDBCMajorVersion();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getJDBCMinorVersion() throws SQLException {
		try {
			return wrapped.getJDBCMinorVersion();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxBinaryLiteralLength() throws SQLException {
		try {
			return wrapped.getMaxBinaryLiteralLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxCatalogNameLength() throws SQLException {
		try {
			return wrapped.getMaxCatalogNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxCharLiteralLength() throws SQLException {
		try {
			return wrapped.getMaxCharLiteralLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnNameLength() throws SQLException {
		try {
			return wrapped.getMaxColumnNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnsInGroupBy() throws SQLException {
		try {
			return wrapped.getMaxColumnsInGroupBy();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnsInIndex() throws SQLException {
		try {
			return wrapped.getMaxColumnsInIndex();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnsInOrderBy() throws SQLException {
		try {
			return wrapped.getMaxColumnsInOrderBy();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnsInSelect() throws SQLException {
		try {
			return wrapped.getMaxColumnsInSelect();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxColumnsInTable() throws SQLException {
		try {
			return wrapped.getMaxColumnsInTable();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxConnections() throws SQLException {
		try {
			return wrapped.getMaxConnections();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxCursorNameLength() throws SQLException {
		try {
			return wrapped.getMaxCursorNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxIndexLength() throws SQLException {
		try {
			return wrapped.getMaxIndexLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxProcedureNameLength() throws SQLException {
		try {
			return wrapped.getMaxProcedureNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxRowSize() throws SQLException {
		try {
			return wrapped.getMaxRowSize();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxSchemaNameLength() throws SQLException {
		try {
			return wrapped.getMaxSchemaNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxStatementLength() throws SQLException {
		try {
			return wrapped.getMaxStatementLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxStatements() throws SQLException {
		try {
			return wrapped.getMaxStatements();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxTableNameLength() throws SQLException {
		try {
			return wrapped.getMaxTableNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxTablesInSelect() throws SQLException {
		try {
			return wrapped.getMaxTablesInSelect();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getMaxUserNameLength() throws SQLException {
		try {
			return wrapped.getMaxUserNameLength();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getNumericFunctions() throws SQLException {
		try {
			return wrapped.getNumericFunctions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		try {
			table = normalizeName(table);
			return new MetadataResultSet(wrapped.getPrimaryKeys("PUBLIC",
					"PUBLIC", table));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		try {
			columnNamePattern = normalizeName(columnNamePattern);
			return wrapped.getProcedureColumns(catalog, schemaPattern,
					procedureNamePattern, columnNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		try {
			return wrapped.getProcedures(catalog, schemaPattern,
					procedureNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getProcedureTerm() throws SQLException {
		try {
			return wrapped.getProcedureTerm();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		try {
			return ((JDBCDatabaseMetaData)wrapped).getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getResultSetHoldability() throws SQLException {
		try {
			return wrapped.getResultSetHoldability();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public ResultSet getSchemas() throws SQLException {
		if (this.connection.isShowSchema()) {
			try {
				return wrapped.getSchemas();
			} catch (SQLException e) {
				throw new UcanaccessSQLException(e);
			}
		}
		throw new FeatureNotSupportedException();
	}

	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		if (this.connection.isShowSchema()) {
			try {
				return wrapped.getSchemas(catalog, schemaPattern);
			} catch (SQLException e) {
				throw new UcanaccessSQLException(e);
			}
		}
		throw new FeatureNotSupportedException();
	}

	public String getSchemaTerm() throws SQLException {
		return null;
	}

	public String getSearchStringEscape() throws SQLException {
		try {
			return wrapped.getSearchStringEscape();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getSQLKeywords() throws SQLException {
		try {
			return wrapped.getSQLKeywords();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getSQLStateType() throws SQLException {
		try {
			return wrapped.getSQLStateType();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getStringFunctions() throws SQLException {
		try {
			return wrapped.getStringFunctions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		try {
			tableNamePattern = normalizeName(tableNamePattern);
			return wrapped.getSuperTables(catalog, schemaPattern,
					tableNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		try {
			return wrapped.getSuperTypes(catalog, schemaPattern,
					typeNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getSystemFunctions() throws SQLException {
		try {
			return wrapped.getSystemFunctions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		try {
			tableNamePattern = normalizeName(tableNamePattern);
			return wrapped.getTablePrivileges(catalog, schemaPattern,
					tableNamePattern);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		try {
			tableNamePattern = normalizeName(tableNamePattern);
			if(this.connection.isShowSchema())
				return wrapped.getTables("PUBLIC", "PUBLIC",
						tableNamePattern, types);
			return new MetadataResultSet(wrapped.getTables("PUBLIC", "PUBLIC",
					tableNamePattern, types));
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getTableTypes() throws SQLException {
		try {
			return wrapped.getTableTypes();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getTimeDateFunctions() throws SQLException {
		try {
			return wrapped.getTimeDateFunctions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getTypeInfo() throws SQLException {
		try {
			return wrapped.getTypeInfo();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		try {
			return wrapped.getUDTs(catalog, schemaPattern, typeNamePattern,
					types);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String getURL() throws SQLException {
		return connection.getUrl();
	}

	public String getUserName() throws SQLException {
		try {
			return wrapped.getUserName();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		try {
			return wrapped.getVersionColumns(catalog, schema, table);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean insertsAreDetected(int type) throws SQLException {
		try {
			return wrapped.insertsAreDetected(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean isCatalogAtStart() throws SQLException {
		try {
			return wrapped.isCatalogAtStart();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean isReadOnly() throws SQLException {
		try {
			return wrapped.isReadOnly();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		try {
			return wrapped.isWrapperFor(iface);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean locatorsUpdateCopy() throws SQLException {
		try {
			return wrapped.locatorsUpdateCopy();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	private String normalizeName(String table) {
		
		if(table == null) return null; 
		if(table.indexOf("%")>=0) return table;
		else return SQLConverter.basicEscapingIdentifier(
				table).toUpperCase();
	}

	public boolean nullPlusNonNullIsNull() throws SQLException {
		try {
			return wrapped.nullPlusNonNullIsNull();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean nullsAreSortedAtEnd() throws SQLException {
		try {
			return wrapped.nullsAreSortedAtEnd();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean nullsAreSortedAtStart() throws SQLException {
		try {
			return wrapped.nullsAreSortedAtStart();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean nullsAreSortedHigh() throws SQLException {
		try {
			return wrapped.nullsAreSortedHigh();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean nullsAreSortedLow() throws SQLException {
		try {
			return wrapped.nullsAreSortedLow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		try {
			return wrapped.othersDeletesAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		try {
			return wrapped.othersInsertsAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		try {
			return wrapped.othersUpdatesAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		try {
			return wrapped.ownDeletesAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		try {
			return wrapped.ownInsertsAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		try {
			return wrapped.ownUpdatesAreVisible(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException {
		try {
			return wrapped.storesLowerCaseIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		try {
			return wrapped.storesLowerCaseQuotedIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException {
		try {
			return wrapped.storesMixedCaseIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		try {
			return wrapped.storesMixedCaseQuotedIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException {
		try {
			return wrapped.storesUpperCaseIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		try {
			return wrapped.storesUpperCaseQuotedIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		try {
			return wrapped.supportsAlterTableWithAddColumn();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		try {
			return wrapped.supportsAlterTableWithDropColumn();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		try {
			return wrapped.supportsANSI92EntryLevelSQL();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsANSI92FullSQL() throws SQLException {
		try {
			return wrapped.supportsANSI92FullSQL();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		try {
			return wrapped.supportsANSI92IntermediateSQL();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsBatchUpdates() throws SQLException {
		try {
			return wrapped.supportsBatchUpdates();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		try {
			return wrapped.supportsCatalogsInDataManipulation();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		try {
			return wrapped.supportsCatalogsInIndexDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		try {
			return wrapped.supportsCatalogsInPrivilegeDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		try {
			return wrapped.supportsCatalogsInProcedureCalls();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		try {
			return wrapped.supportsCatalogsInTableDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsColumnAliasing() throws SQLException {
		try {
			return wrapped.supportsColumnAliasing();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsConvert() throws SQLException {
		try {
			return wrapped.supportsConvert();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		try {
			return wrapped.supportsConvert(fromType, toType);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCoreSQLGrammar() throws SQLException {
		try {
			return wrapped.supportsCoreSQLGrammar();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException {
		try {
			return wrapped.supportsCorrelatedSubqueries();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		try {
			return wrapped
					.supportsDataDefinitionAndDataManipulationTransactions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		try {
			return wrapped.supportsDataManipulationTransactionsOnly();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		try {
			return wrapped.supportsDifferentTableCorrelationNames();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException {
		try {
			return wrapped.supportsExpressionsInOrderBy();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException {
		try {
			return wrapped.supportsExtendedSQLGrammar();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsFullOuterJoins() throws SQLException {
		try {
			return wrapped.supportsFullOuterJoins();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsGetGeneratedKeys() throws SQLException {
		try {
			return wrapped.supportsGetGeneratedKeys();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsGroupBy() throws SQLException {
		try {
			return wrapped.supportsGroupBy();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException {
		try {
			return wrapped.supportsGroupByBeyondSelect();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsGroupByUnrelated() throws SQLException {
		try {
			return wrapped.supportsGroupByUnrelated();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		try {
			return wrapped.supportsIntegrityEnhancementFacility();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsLikeEscapeClause() throws SQLException {
		try {
			return wrapped.supportsLikeEscapeClause();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsLimitedOuterJoins() throws SQLException {
		try {
			return wrapped.supportsLimitedOuterJoins();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException {
		try {
			return wrapped.supportsMinimumSQLGrammar();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		try {
			return wrapped.supportsMixedCaseIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		try {
			return wrapped.supportsMixedCaseQuotedIdentifiers();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMultipleOpenResults() throws SQLException {
		try {
			return wrapped.supportsMultipleOpenResults();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMultipleResultSets() throws SQLException {
		try {
			return wrapped.supportsMultipleResultSets();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsMultipleTransactions() throws SQLException {
		try {
			return wrapped.supportsMultipleTransactions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsNamedParameters() throws SQLException {
		try {
			return wrapped.supportsNamedParameters();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsNonNullableColumns() throws SQLException {
		try {
			return wrapped.supportsNonNullableColumns();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		try {
			return wrapped.supportsOpenCursorsAcrossCommit();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		try {
			return wrapped.supportsOpenCursorsAcrossRollback();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		try {
			return wrapped.supportsOpenStatementsAcrossCommit();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		try {
			return wrapped.supportsOpenStatementsAcrossRollback();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOrderByUnrelated() throws SQLException {
		try {
			return wrapped.supportsOrderByUnrelated();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsOuterJoins() throws SQLException {
		try {
			return wrapped.supportsOuterJoins();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsPositionedDelete() throws SQLException {
		try {
			return wrapped.supportsPositionedDelete();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsPositionedUpdate() throws SQLException {
		try {
			return wrapped.supportsPositionedUpdate();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		try {
			return wrapped.supportsResultSetConcurrency(type, concurrency);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		try {
			return wrapped.supportsResultSetHoldability(holdability);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsResultSetType(int type) throws SQLException {
		try {
			return wrapped.supportsResultSetType(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSavepoints() throws SQLException {
		try {
			return wrapped.supportsSavepoints();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException {
		try {
			return wrapped.supportsSchemasInDataManipulation();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		try {
			return wrapped.supportsSchemasInIndexDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		try {
			return wrapped.supportsSchemasInPrivilegeDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		try {
			return wrapped.supportsSchemasInProcedureCalls();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		try {
			return wrapped.supportsSchemasInTableDefinitions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSelectForUpdate() throws SQLException {
		try {
			return wrapped.supportsSelectForUpdate();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsStatementPooling() throws SQLException {
		try {
			return wrapped.supportsStatementPooling();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		try {
			return wrapped.supportsStoredFunctionsUsingCallSyntax();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsStoredProcedures() throws SQLException {
		try {
			return wrapped.supportsStoredProcedures();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException {
		try {
			return wrapped.supportsSubqueriesInComparisons();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSubqueriesInExists() throws SQLException {
		try {
			return wrapped.supportsSubqueriesInExists();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSubqueriesInIns() throws SQLException {
		try {
			return wrapped.supportsSubqueriesInIns();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		try {
			return wrapped.supportsSubqueriesInQuantifieds();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsTableCorrelationNames() throws SQLException {
		try {
			return wrapped.supportsTableCorrelationNames();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		try {
			return wrapped.supportsTransactionIsolationLevel(level);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsTransactions() throws SQLException {
		try {
			return wrapped.supportsTransactions();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsUnion() throws SQLException {
		try {
			return wrapped.supportsUnion();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean supportsUnionAll() throws SQLException {
		try {
			return wrapped.supportsUnionAll();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			return wrapped.unwrap(iface);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean updatesAreDetected(int type) throws SQLException {
		try {
			return wrapped.updatesAreDetected(type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	
	public boolean usesLocalFilePerTable() throws SQLException {
		try {
			return wrapped.usesLocalFilePerTable();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	
	public boolean usesLocalFiles() throws SQLException {
		try {
			return wrapped.usesLocalFiles();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
}
