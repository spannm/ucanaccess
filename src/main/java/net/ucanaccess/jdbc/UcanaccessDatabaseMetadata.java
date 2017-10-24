/*
 Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
NOTICE:
Some of the UcanaccessDatabaseMetadata methods have been originally inspired by the hsqldb DatabaseMetaData implementation.
They have been then modified and adapted so that they are integrated with UCanAccess, in a consistent manner.
The  Hsqldb  project is licensed under a BSD based license.
*/
package net.ucanaccess.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.hsqldb.jdbc.JDBCDatabaseMetaData;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

public class UcanaccessDatabaseMetadata implements DatabaseMetaData {
    private static final String  SELECT_BASE  = "SELECT * FROM INFORMATION_SCHEMA.";
    private static final String  NATIVE_ALIAS = " l.";
    private static final String  CUSTOM_ALIAS = " r.";
    private static final String  CAST_EXPR    = "CAST(null AS VARCHAR(50)) AS ";
    private UcanaccessConnection connection;
    private DatabaseMetaData     wrapped;

    public UcanaccessDatabaseMetadata(DatabaseMetaData wrapped, UcanaccessConnection connection) {
        this.wrapped = wrapped;
        this.connection = connection;
    }

    private String from(String left, String right) {
        StringBuffer sb = new StringBuffer(" FROM ").append("INFORMATION_SCHEMA.").append(left).append(" l INNER JOIN ")
                .append("UCA_METADATA.").append(right).append(" r ");
        return sb.toString();
    }

    private String in(String prefix, String field, Object[] options) {
        if (options == null || options.length == 0) {
            return "";
        }
        StringBuffer sb = new StringBuffer(" AND ").append(prefix).append(field).append(" IN ").append("(");
        String comma = "";
        for (int i = 0; i < options.length; i++) {
            Object norm = options[i];
            String val = norm instanceof String ? "'" + norm.toString().toUpperCase() + "'" : norm.toString();
            sb.append(comma).append(val);
            comma = ",";
        }
        sb.append(")");
        return sb.toString();
    }

    private String nAlias(String s) {
        return NATIVE_ALIAS + s;
    }

    private String cAlias(String s) {
        return CUSTOM_ALIAS + s;
    }

    private String on(List<String> left, List<String> right) {
        StringBuffer sb = new StringBuffer(" ON(");
        Iterator<String> il = left.iterator();
        Iterator<String> ir = right.iterator();
        String and = "";
        while (il.hasNext() && ir.hasNext()) {
            sb.append(and).append(NATIVE_ALIAS).append(il.next()).append("=").append(CUSTOM_ALIAS).append(ir.next());
            and = " AND ";
        }
        sb.append(") ");
        return sb.toString();
    }

    private static String and(String left, String op, String right) {
        return and(left, op, right, " AND ");
    }

    private static String and(String left, String op, String right, String and) {
        return and(left, op, right, and, true);
    }

    private static String and(String left, String op, String right, boolean apos) {
        return and(left, op, right, " AND ", apos);
    }

    private static String and(String left, String op, String right, String and, boolean apos) {
        if (right == null) {
            return "";
        }
        StringBuffer sb = new StringBuffer(and);
        if (right.length() == 0) {
            return sb.append(left).append(" IS NULL ").toString();
        }
        String exp = right == null ? null : (apos ? "'" + right + "'" : right);
        sb.append(left).append(" ");
        if ("LIKE".equals(op)) {
            if (exp.indexOf('_') < 0 && exp.indexOf('%') < 0) {
                sb.append(" = ").append(exp);
            } else {
                sb.append(" LIKE ").append(exp);
                sb.append(" ESCAPE '\\'");
            }
        } else {
            sb.append(op).append(' ').append(exp);
        }
        return sb.toString();
    }

    private String select(String htableName, List<String> exclude, List<String> replace) throws SQLException {
        ResultSetMetaData rsmd = executeQuery(SELECT_BASE + htableName + " WHERE 1=0").getMetaData();
        String comma = "";
        StringBuffer sb = new StringBuffer("SELECT ");

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            String cn = rsmd.getColumnName(i);
            if (exclude.contains(cn)) {
                String es = replace.get(exclude.indexOf(cn));
                sb.append(comma);

                if (es == null) {
                    sb.append(CAST_EXPR + cn);
                } else if (es.equals("PUBLIC")) {
                    sb.append("'PUBLIC' AS " + cn);
                }

                else if (es.startsWith(CAST_EXPR) || es.equals("PUBLIC")) {
                    sb.append(es);
                } else {
                    String suffix = es.indexOf(".") > 0 ? "" : "r.";
                    sb.append(suffix).append(es).append(" AS ").append(cn);
                }
            } else {
                sb.append(comma).append("l.").append(cn);

            }
            comma = ",";
        }
        return sb.toString();
    }

    private ResultSet executeQuery(String sql) throws SQLException {
        this.connection.checkLastModified();
        Statement st = connection.getHSQLDBConnection().createStatement();
        ResultSet rs = st.executeQuery(sql);
        return rs;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        try {
            return wrapped.allProceduresAreCallable();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        try {
            return wrapped.allTablesAreSelectable();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        try {
            return wrapped.autoCommitFailureClosesAllResultSets();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        try {
            return wrapped.dataDefinitionCausesTransactionCommit();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        try {
            return wrapped.dataDefinitionIgnoredInTransactions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        try {
            return wrapped.deletesAreDetected(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        try {
            return wrapped.doesMaxRowSizeIncludeBlobs();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        try {
            return wrapped.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        table = SQLConverter.escapeIdentifier(table).toUpperCase();

        try {

            Integer[] scopeArr = new Integer[] { bestRowTemporary, bestRowTransaction, bestRowSession };

            String nullableS = (nullable) ? null : String.valueOf(columnNoNulls);
            StringBuffer sql =

                    new StringBuffer(select("SYSTEM_BESTROWIDENTIFIER",

                            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"),
                            Arrays.asList(null, null, "TABLE_NAME", "COLUMN_NAME")))
                                    .append(from("SYSTEM_BESTROWIDENTIFIER", "COLUMNS_VIEW"))

                                    .append(on(Arrays.asList("TABLE_NAME", "COLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(and("TABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_SCHEM", "=", schema)).append(and("TABLE_NAME", "=", table))
                                    .append(and("NULLABLE", "=", nullableS, false))
                                    .append(in(NATIVE_ALIAS, "SCOPE", scopeArr));

            return executeQuery(sql.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
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

    @Override
    public String getCatalogSeparator() throws SQLException {
        try {
            return wrapped.getCatalogSeparator();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        try {
            return wrapped.getCatalogTerm();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        try {
            String sql = "SELECT * FROM UCA_METADATA.PROP";

            return executeQuery(sql);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        try {
            if (table == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "table");
            }
            columnNamePattern = normalizeName(columnNamePattern);
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return this.wrapped.getColumnPrivileges(catalog, schema, table, columnNamePattern);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;

            StringBuffer select = new StringBuffer("SELECT " + cat + " TABLE_CAT, " + schem + " TABLE_SCHEM,")
                    .append(cAlias("TABLE_NAME")).append(",").append(cAlias("COLUMN_NAME")).append(",")
                    .append(nAlias("GRANTOR")).append(",").append(nAlias("GRANTEE")).append(",")
                    .append(nAlias("PRIVILEGE_TYPE PRIVILEGE")).append(",").append(nAlias("IS_GRANTABLE"))
                    .append(from("COLUMN_PRIVILEGES", "COLUMNS_VIEW"))
                    .append(on(Arrays.asList("TABLE_NAME", "COLUMN_NAME"),
                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                    .append(and("TABLE_CATALOG", "=", "PUBLIC", " WHERE "))
                    .append(and("TABLE_SCHEMA", "=", "PUBLIC") + and("TABLE_NAME", "=", table))
                    .append(and("COLUMN_NAME", "LIKE", columnNamePattern));

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            columnNamePattern = normalizeName(columnNamePattern);
            tableNamePattern = normalizeName(tableNamePattern);
            if (invokeWrapper(catalog, schemaPattern)) {
                return this.wrapped.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;

            StringBuffer select = new StringBuffer(select("SYSTEM_COLUMNS",
                    Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "COLUMN_DEF",
                            "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"),
                    Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME", "COLUMN_DEF", "IS_AUTOINCREMENT",
                            "IS_GENERATEDCOLUMN"))).append(",").append(this.cAlias("ORIGINAL_TYPE"))
                                    .append(from("SYSTEM_COLUMNS", "COLUMNS_VIEW"))

                                    .append(on(Arrays.asList("TABLE_NAME", "COLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(and("TABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_SCHEM", "=", "PUBLIC"))
                                    .append(and("TABLE_NAME", "LIKE", tableNamePattern))
                                    .append(and("COLUMN_NAME", "LIKE", columnNamePattern));

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        try {
            if (parentTable == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "parentTable");
            }
            if (foreignTable == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "foreignTable");
            }
            parentTable = normalizeName(parentTable);
            foreignTable = normalizeName(foreignTable);
            if (this.invokeWrapper(parentCatalog, parentSchema)) {
                return wrapped.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
                        foreignSchema, foreignTable);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;
            StringBuffer select = new StringBuffer(select("SYSTEM_CROSSREFERENCE",
                    Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                            "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME"),
                    Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME", cat, schem, "v.TABLE_NAME",
                            "v.COLUMN_NAME"))).append(from("SYSTEM_CROSSREFERENCE", "COLUMNS_VIEW"))

                                    .append(on(Arrays.asList("PKTABLE_NAME", "PKCOLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(" INNER JOIN UCA_METADATA.COLUMNS_VIEW v ON( l.FKTABLE_NAME= v.ESCAPED_TABLE_NAME AND  l.FKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)")

                                    .append(and("PKTABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("PKTABLE_SCHEM", "=", "PUBLIC"))
                                    .append(and("PKTABLE_NAME", "=", parentTable))
                                    .append(and("FKTABLE_CAT", "=", "PUBLIC"))
                                    .append(and("FKTABLE_SCHEM", "=", "PUBLIC"))
                                    .append(and("FKTABLE_NAME", "=", foreignTable))
                                    .append(" ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMinorVersion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getDatabaseProductName() throws SQLException {

        return "UCanAccess driver for Microsoft Access databases using HSQLDB";

    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        try {
            return this.connection.getDbIO().getFileFormat().toString();
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        try {
            return wrapped.getDefaultTransactionIsolation();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Ucanaccess";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        try {
            String version = this.getClass().getPackage().getImplementationVersion();
            return version == null ? "3.x.x" : version;
        } catch (Exception e) {
            return "2.x.x";
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "table");
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return this.wrapped.getExportedKeys(catalog, schema, table);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;
            StringBuffer select = new StringBuffer(select("SYSTEM_CROSSREFERENCE",
                    Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                            "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME"),
                    Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME", cat, schem, "v.TABLE_NAME",
                            "v.COLUMN_NAME"))).append(from("SYSTEM_CROSSREFERENCE", "COLUMNS_VIEW"))

                                    .append(on(Arrays.asList("PKTABLE_NAME", "PKCOLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(" INNER JOIN UCA_METADATA.COLUMNS_VIEW v ON( l.FKTABLE_NAME= v.ESCAPED_TABLE_NAME AND  l.FKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)")

                                    .append(and("PKTABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("PKTABLE_SCHEM", "=", "PUBLIC")).append(and("PKTABLE_NAME", "=", table))
                                    .append(" ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        try {
            return wrapped.getExtraNameCharacters();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            columnNamePattern = normalizeName(columnNamePattern);
            return wrapped.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        try {
            return wrapped.getFunctions(catalog, schemaPattern, functionNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "table");
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return this.wrapped.getImportedKeys(catalog, schema, table);
            }

            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;
            StringBuffer select = new StringBuffer(select("SYSTEM_CROSSREFERENCE",
                    Arrays.asList("FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "PKTABLE_CAT",
                            "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME"),
                    Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME", cat, schem, "v.TABLE_NAME",
                            "v.COLUMN_NAME"))).append(from("SYSTEM_CROSSREFERENCE", "COLUMNS_VIEW"))

                                    .append(on(Arrays.asList("FKTABLE_NAME", "FKCOLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(" INNER JOIN UCA_METADATA.COLUMNS_VIEW v ON( l.PKTABLE_NAME= v.ESCAPED_TABLE_NAME AND  l.PKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)")

                                    .append(and("FKTABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("FKTABLE_SCHEM", "=", "PUBLIC")).append(and("FKTABLE_NAME", "=", table))
                                    .append(" ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");
            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        try {
            if (table == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "table");
            }
            table = normalizeName(table);
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;
            String nuS = (unique) ? "AND NON_UNIQUE IS FALSE" : "";
            StringBuffer select = new StringBuffer(
                    select("SYSTEM_INDEXINFO", Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"),
                            Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME")))
                                    .append(from("SYSTEM_INDEXINFO", "COLUMNS_VIEW"))
                                    .append(on(Arrays.asList("TABLE_NAME", "COLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(and("TABLE_CAT", "=", "PUBLIC"))
                                    .append(and("TABLE_SCHEM", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_NAME", "=", table)).append(nuS);

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMajorVersion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMinorVersion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxBinaryLiteralLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        try {
            return wrapped.getMaxCatalogNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxCharLiteralLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        try {
            return wrapped.getMaxColumnNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInGroupBy();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        try {
            return wrapped.getMaxColumnsInIndex();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInOrderBy();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        try {
            return wrapped.getMaxColumnsInSelect();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        try {
            return wrapped.getMaxColumnsInTable();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxConnections() throws SQLException {
        try {
            return wrapped.getMaxConnections();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        try {
            return wrapped.getMaxCursorNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        try {
            return wrapped.getMaxIndexLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        try {
            return wrapped.getMaxProcedureNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        try {
            return wrapped.getMaxRowSize();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        try {
            return wrapped.getMaxSchemaNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        try {
            return wrapped.getMaxStatementLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxStatements() throws SQLException {
        try {
            return wrapped.getMaxStatements();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        try {
            return wrapped.getMaxTableNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        try {
            return wrapped.getMaxTablesInSelect();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        try {
            return wrapped.getMaxUserNameLength();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        try {
            return wrapped.getNumericFunctions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    private boolean invokeWrapper(String catalog, String schema) {

        return (this.connection.isShowSchema()
                && ((catalog != null || schema != null) && (!"PUBLIC".equals(catalog) || !"PUBLIC".equals(schema))));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new UcanaccessSQLException(ExceptionMessages.PARAMETER_NULL, "table");
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return this.wrapped.getPrimaryKeys(catalog, schema, table);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;
            StringBuffer select = new StringBuffer(
                    select("SYSTEM_PRIMARYKEYS", Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"),
                            Arrays.asList(cat, schem, "TABLE_NAME", "COLUMN_NAME")))
                                    .append(from("SYSTEM_PRIMARYKEYS", "COLUMNS_VIEW"))
                                    .append(on(Arrays.asList("TABLE_NAME", "COLUMN_NAME"),
                                            Arrays.asList("ESCAPED_TABLE_NAME", "ESCAPED_COLUMN_NAME")))
                                    .append(and("TABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_SCHEM", "=", "PUBLIC")).append(and("TABLE_NAME", "=", table));
            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            columnNamePattern = normalizeName(columnNamePattern);
            return wrapped.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        try {
            return wrapped.getProcedures(catalog, schemaPattern, procedureNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        try {
            return wrapped.getProcedureTerm();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            return ((JDBCDatabaseMetaData) wrapped).getPseudoColumns(catalog, schemaPattern, tableNamePattern,
                    columnNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            return wrapped.getResultSetHoldability();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new FeatureNotSupportedException();
    }

    @Override
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

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (this.connection.isShowSchema()) {
            try {
                return wrapped.getSchemas(catalog, schemaPattern);
            } catch (SQLException e) {
                throw new UcanaccessSQLException(e);
            }
        }
        throw new FeatureNotSupportedException();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        try {
            return wrapped.getSearchStringEscape();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        try {
            return wrapped.getSQLKeywords();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getSQLStateType() throws SQLException {
        try {
            return wrapped.getSQLStateType();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getStringFunctions() throws SQLException {
        try {
            return wrapped.getStringFunctions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);
            return wrapped.getSuperTables(catalog, schemaPattern, tableNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        try {
            return wrapped.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        try {
            return wrapped.getSystemFunctions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);
            StringBuffer select = new StringBuffer(
                    select("TABLE_PRIVILEGES", Arrays.asList("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"),
                            Arrays.asList(CAST_EXPR + " TABLE_CAT ", CAST_EXPR + " TABLE_SCHEM", "TABLE_NAME")))
                                    .append(from("TABLE_PRIVILEGES", "TABLES"))

                                    .append(on(Arrays.asList("TABLE_NAME"), Arrays.asList("ESCAPED_TABLE_NAME")))
                                    .append(and("TABLE_CATALOG", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_SCHEMA", "=", "PUBLIC"))
                                    .append(and("TABLE_NAME", "LIKE", tableNamePattern));

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);

            if (invokeWrapper(catalog, schemaPattern)) {
                return this.wrapped.getTables(catalog, schemaPattern, tableNamePattern, types);
            }
            String cat = this.connection.isShowSchema() ? "PUBLIC" : null;
            String schem = this.connection.isShowSchema() ? "PUBLIC" : null;

            StringBuffer select =
                    new StringBuffer(select("SYSTEM_TABLES", Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"),
                            Arrays.asList(cat, schem, "TABLE_NAME"))).append(from("SYSTEM_TABLES", "TABLES"))

                                    .append(on(Arrays.asList("TABLE_NAME"), Arrays.asList("ESCAPED_TABLE_NAME")))
                                    .append(and("TABLE_CAT", "=", "PUBLIC", " WHERE "))
                                    .append(and("TABLE_SCHEM", "=", "PUBLIC"))
                                    .append(and("TABLE_NAME", "LIKE", tableNamePattern))
                                    .append(in(CUSTOM_ALIAS, "TYPE", types));

            return executeQuery(select.toString());
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            return wrapped.getTableTypes();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        try {
            return wrapped.getTimeDateFunctions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try {
            return wrapped.getTypeInfo();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        try {
            return wrapped.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        try {
            return wrapped.getUserName();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getVersionColumns(catalog, schema, table);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        try {
            return wrapped.insertsAreDetected(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        try {
            return wrapped.isCatalogAtStart();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            return wrapped.isReadOnly();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            return wrapped.isWrapperFor(iface);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        try {
            return wrapped.locatorsUpdateCopy();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public static String normalizeName(String name) {
        if (name == null || name.trim().length() == 0) {
            return name;
        }
        if (name.indexOf("%") >= 0) {
            return name.toUpperCase();
        } else {
            if (name.startsWith("\"") && name.endsWith("\"")) {
                String stb = name.substring(1, name.length() - 1).toUpperCase();
                if (SQLConverter.isListedAsKeyword(stb)) {
                    return stb;
                }
            }
            if (SQLConverter.isListedAsKeyword(name)) {
                return name;
            }
            return SQLConverter.preEscapingIdentifier(name);
        }
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        try {
            return wrapped.nullPlusNonNullIsNull();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtEnd();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtStart();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        try {
            return wrapped.nullsAreSortedHigh();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        try {
            return wrapped.nullsAreSortedLow();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersDeletesAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersInsertsAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersUpdatesAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownDeletesAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownInsertsAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownUpdatesAreVisible(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseQuotedIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseQuotedIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseQuotedIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithAddColumn();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithDropColumn();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92EntryLevelSQL();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92FullSQL();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92IntermediateSQL();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        try {
            return wrapped.supportsBatchUpdates();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsCatalogsInDataManipulation();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInIndexDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInPrivilegeDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsCatalogsInProcedureCalls();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInTableDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        try {
            return wrapped.supportsColumnAliasing();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        try {
            return wrapped.supportsConvert();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        try {
            return wrapped.supportsConvert(fromType, toType);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsCoreSQLGrammar();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        try {
            return wrapped.supportsCorrelatedSubqueries();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        try {
            return wrapped.supportsDataDefinitionAndDataManipulationTransactions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        try {
            return wrapped.supportsDataManipulationTransactionsOnly();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsDifferentTableCorrelationNames();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        try {
            return wrapped.supportsExpressionsInOrderBy();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsExtendedSQLGrammar();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        try {
            return wrapped.supportsFullOuterJoins();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        try {
            return wrapped.supportsGetGeneratedKeys();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        try {
            return wrapped.supportsGroupBy();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        try {
            return wrapped.supportsGroupByBeyondSelect();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        try {
            return wrapped.supportsGroupByUnrelated();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        try {
            return wrapped.supportsIntegrityEnhancementFacility();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        try {
            return wrapped.supportsLikeEscapeClause();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        try {
            return wrapped.supportsLimitedOuterJoins();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsMinimumSQLGrammar();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseQuotedIdentifiers();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        try {
            return wrapped.supportsMultipleOpenResults();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        try {
            return wrapped.supportsMultipleResultSets();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        try {
            return wrapped.supportsMultipleTransactions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        try {
            return wrapped.supportsNamedParameters();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        try {
            return wrapped.supportsNonNullableColumns();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossCommit();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossRollback();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossCommit();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossRollback();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        try {
            return wrapped.supportsOrderByUnrelated();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        try {
            return wrapped.supportsOuterJoins();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        try {
            return wrapped.supportsPositionedDelete();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        try {
            return wrapped.supportsPositionedUpdate();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        try {
            return wrapped.supportsResultSetConcurrency(type, concurrency);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        try {
            return wrapped.supportsResultSetHoldability(holdability);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        try {
            return wrapped.supportsResultSetType(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        try {
            return wrapped.supportsSavepoints();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsSchemasInDataManipulation();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInIndexDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInPrivilegeDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsSchemasInProcedureCalls();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInTableDefinitions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        try {
            return wrapped.supportsSelectForUpdate();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        try {
            return wrapped.supportsStatementPooling();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        try {
            return wrapped.supportsStoredFunctionsUsingCallSyntax();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        try {
            return wrapped.supportsStoredProcedures();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInComparisons();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInExists();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInIns();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInQuantifieds();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsTableCorrelationNames();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        try {
            return wrapped.supportsTransactionIsolationLevel(level);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        try {
            return wrapped.supportsTransactions();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        try {
            return wrapped.supportsUnion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        try {
            return wrapped.supportsUnionAll();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        try {
            return wrapped.updatesAreDetected(type);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        try {
            return wrapped.usesLocalFilePerTable();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        try {
            return wrapped.usesLocalFiles();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }
}
