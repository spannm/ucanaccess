package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.*;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.exception.FeatureNotSupportedRuntimeException;
import net.ucanaccess.exception.InvalidParameterException;
import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UcanaccessDatabaseMetadata implements DatabaseMetaData {
    private static final String        SELECT_BASE  = "SELECT * FROM INFORMATION_SCHEMA.";
    private static final String        NATIVE_ALIAS = " l.";
    private static final String        CUSTOM_ALIAS = " r.";
    private static final String        CAST_EXPR    = "CAST(null AS VARCHAR(50)) AS ";

    private final UcanaccessConnection connection;
    private final DatabaseMetaData     wrapped;

    public UcanaccessDatabaseMetadata(DatabaseMetaData _wrapped, UcanaccessConnection _connection) {
        wrapped = _wrapped;
        connection = _connection;
    }

    private String from(String left, String right) {
        return " FROM " + "INFORMATION_SCHEMA." + left + " l INNER JOIN UCA_METADATA." + right + " r ";
    }

    private String in(String prefix, String field, Object[] options) {
        if (options == null || options.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" AND ").append(prefix).append(field).append(" IN ").append("(");
        String comma = "";
        for (Object norm : options) {
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

    private static String on(List<String> left, List<String> right) {
        StringBuilder sb = new StringBuilder(" ON(");
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
        StringBuilder sb = new StringBuilder(and);
        if (right.length() == 0) {
            return sb.append(left).append(" IS NULL ").toString();
        }
        String exp = apos ? "'" + right + "'" : right;
        sb.append(left).append(" ");
        if (LIKE.equals(op)) {
            if (exp.indexOf('_') < 0 && exp.indexOf('%') < 0) {
                sb.append(" = ").append(exp);
            } else {
                sb.append(" LIKE ").append(exp)
                  .append(" ESCAPE '\\'");
            }
        } else {
            sb.append(op).append(' ').append(exp);
        }
        return sb.toString();
    }

    private String select(String htableName, List<String> exclude, List<String> replace) throws SQLException {
        ResultSetMetaData rsmd = executeQuery(SELECT_BASE + htableName + " WHERE 1=0").getMetaData();
        String comma = "";
        StringBuilder sb = new StringBuilder("SELECT ");

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            String cn = rsmd.getColumnName(i);
            if (exclude.contains(cn)) {
                String es = replace.get(exclude.indexOf(cn));
                sb.append(comma);

                if (es == null) {
                    sb.append(CAST_EXPR).append(cn);
                } else if (PUBLIC.equals(es)) {
                    sb.append("'PUBLIC' AS ").append(cn);
                } else if (es.startsWith(CAST_EXPR)) {
                    sb.append(es);
                } else {
                    String suffix = es.indexOf('.') > 0 ? "" : "r.";
                    sb.append(suffix).append(es).append(" AS ").append(cn);
                }
            } else {
                sb.append(comma).append("l.").append(cn);

            }
            comma = ",";
        }
        return sb.toString();
    }

    private ResultSet executeQuery(String _sql) throws SQLException {
        connection.checkLastModified();
        try (Statement st = connection.getHSQLDBConnection().createStatement()) {
            return st.executeQuery(_sql);
        }
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        try {
            return wrapped.allProceduresAreCallable();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        try {
            return wrapped.allTablesAreSelectable();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        try {
            return wrapped.autoCommitFailureClosesAllResultSets();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        try {
            return wrapped.dataDefinitionCausesTransactionCommit();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        try {
            return wrapped.dataDefinitionIgnoredInTransactions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        try {
            return wrapped.deletesAreDetected(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        try {
            return wrapped.doesMaxRowSizeIncludeBlobs();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        try {
            return wrapped.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        table = SQLConverter.escapeIdentifier(table).toUpperCase();

        try {

            Integer[] scopeArr = new Integer[] {bestRowTemporary, bestRowTransaction, bestRowSession};

            String nullableS = nullable ? null : String.valueOf(columnNoNulls);
            String sql = select("SYSTEM_BESTROWIDENTIFIER",
                    List.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME),
                    Arrays.asList(null, null, TABLE_NAME, COLUMN_NAME))
                    + from("SYSTEM_BESTROWIDENTIFIER", COLUMNS_VIEW)
                    + on(List.of(TABLE_NAME, COLUMN_NAME), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                    + and(TABLE_CAT, "=", PUBLIC, " WHERE ")
                    + and(TABLE_SCHEM, "=", schema) + and(TABLE_NAME, "=", table)
                    + and("NULLABLE", "=", nullableS, false)
                    + in(NATIVE_ALIAS, "SCOPE", scopeArr);

            return executeQuery(sql);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (connection.isShowSchema()) {
            try {
                return wrapped.getCatalogs();
            } catch (SQLException _ex) {
                throw new UcanaccessSQLException(_ex);
            }
        }
        throw new FeatureNotSupportedRuntimeException();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        try {
            return wrapped.getCatalogSeparator();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        try {
            return wrapped.getCatalogTerm();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        try {
            return executeQuery("SELECT * FROM UCA_METADATA.PROP");
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        try {
            if (table == null) {
                throw new InvalidParameterException("table", table);
            }
            columnNamePattern = normalizeName(columnNamePattern);
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return wrapped.getColumnPrivileges(catalog, schema, table, columnNamePattern);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;

            String s = "SELECT " + cat + " TABLE_CAT, " + schem + " TABLE_SCHEM,";
            String select = s
                + cAlias(TABLE_NAME) + "," + cAlias(COLUMN_NAME) + ","
                + nAlias("GRANTOR") + "," + nAlias("GRANTEE") + ","
                + nAlias("PRIVILEGE_TYPE PRIVILEGE") + "," + nAlias("IS_GRANTABLE")
                + from("COLUMN_PRIVILEGES", COLUMNS_VIEW)
                + on(List.of(TABLE_NAME, COLUMN_NAME),
                    List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + and(TABLE_CATALOG, "=", PUBLIC, " WHERE ")
                + and("TABLE_SCHEMA", "=", PUBLIC)
                + and(TABLE_NAME, "=", table)
                + and(COLUMN_NAME, LIKE, columnNamePattern);

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try {
            columnNamePattern = normalizeName(columnNamePattern);
            tableNamePattern = normalizeName(tableNamePattern);
            if (invokeWrapper(catalog, schemaPattern)) {
                return wrapped.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;

            String select = select("SYSTEM_COLUMNS",
                List.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, "COLUMN_DEF", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"),
                Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME, "COLUMN_DEF", "IS_AUTOINCREMENT",
                    "IS_GENERATEDCOLUMN"))
                + "," + cAlias("ORIGINAL_TYPE")
                + from("SYSTEM_COLUMNS", COLUMNS_VIEW)
                + on(List.of(TABLE_NAME, COLUMN_NAME), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + and(TABLE_CAT, "=", PUBLIC, " WHERE ")
                + and(TABLE_SCHEM, "=", PUBLIC)
                + and(TABLE_NAME, LIKE, tableNamePattern)
                + and(COLUMN_NAME, LIKE, columnNamePattern);

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
        String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        try {
            if (parentTable == null) {
                throw new InvalidParameterException("parentTable", parentTable);
            } else if (foreignTable == null) {
                throw new InvalidParameterException("foreignTable", foreignTable);
            }
            parentTable = normalizeName(parentTable);
            foreignTable = normalizeName(foreignTable);
            if (invokeWrapper(parentCatalog, parentSchema)) {
                return wrapped.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
                    foreignSchema, foreignTable);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;
            String select = select("SYSTEM_CROSSREFERENCE",
                List.of("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                    "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME"),
                Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME, cat, schem, "v.TABLE_NAME", "v.COLUMN_NAME"))
                + from("SYSTEM_CROSSREFERENCE", COLUMNS_VIEW)
                + on(List.of("PKTABLE_NAME", "PKCOLUMN_NAME"), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + " INNER JOIN UCA_METADATA.COLUMNS_VIEW v "
                + "ON( l.FKTABLE_NAME= v.ESCAPED_TABLE_NAME AND l.FKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)"
                + and("PKTABLE_CAT", "=", PUBLIC, " WHERE ")
                + and("PKTABLE_SCHEM", "=", PUBLIC)
                + and("PKTABLE_NAME", "=", parentTable)
                + and("FKTABLE_CAT", "=", PUBLIC)
                + and("FKTABLE_SCHEM", "=", PUBLIC)
                + and("FKTABLE_NAME", "=", foreignTable)
                + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMajorVersion();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        try {
            return wrapped.getDatabaseMinorVersion();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getDatabaseProductName() {

        return "UCanAccess driver for Microsoft Access databases using HSQLDB";

    }

    @Override
    public String getDatabaseProductVersion() {
        try {
            return connection.getDbIO().getFileFormat().toString();
        } catch (IOException _ex) {
            return "";
        }
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        try {
            return wrapped.getDefaultTransactionIsolation();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
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
    public String getDriverName() {
        return "Ucanaccess";
    }

    @Override
    public String getDriverVersion() {
        try {
            String version = getClass().getPackage().getImplementationVersion();
            return version == null ? "3.x.x" : version;
        } catch (Exception _ex) {
            return "2.x.x";
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new InvalidParameterException("table", table);
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return wrapped.getExportedKeys(catalog, schema, table);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;
            String select = select("SYSTEM_CROSSREFERENCE",
                List.of("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                    "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME"),
                Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME, cat, schem, "v.TABLE_NAME", "v.COLUMN_NAME"))
                + from("SYSTEM_CROSSREFERENCE", COLUMNS_VIEW)
                + on(List.of("PKTABLE_NAME", "PKCOLUMN_NAME"), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + " INNER JOIN UCA_METADATA.COLUMNS_VIEW v "
                + "ON( l.FKTABLE_NAME= v.ESCAPED_TABLE_NAME AND l.FKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)"
                + and("PKTABLE_CAT", "=", PUBLIC, " WHERE ")
                + and("PKTABLE_SCHEM", "=", PUBLIC) + and("PKTABLE_NAME", "=", table)
                + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        try {
            return wrapped.getExtraNameCharacters();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getFunctionColumns(String _catalog, String _schemaPattern,
        String _functionNamePattern, String _columnNamePattern) throws SQLException {
        try {
            _columnNamePattern = normalizeName(_columnNamePattern);
            return wrapped.getFunctionColumns(_catalog, _schemaPattern, _functionNamePattern, _columnNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        try {
            return wrapped.getFunctions(catalog, schemaPattern, functionNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getIdentifierQuoteString() {
        return "`";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new InvalidParameterException("table", table);
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return wrapped.getImportedKeys(catalog, schema, table);
            }

            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;
            String select = select("SYSTEM_CROSSREFERENCE",
                List.of("FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "PKTABLE_CAT",
                    "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME"),
                Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME, cat, schem, "v.TABLE_NAME", "v.COLUMN_NAME"))
                + from("SYSTEM_CROSSREFERENCE", COLUMNS_VIEW)
                + on(List.of("FKTABLE_NAME", "FKCOLUMN_NAME"), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + " INNER JOIN UCA_METADATA.COLUMNS_VIEW v "
                + "ON( l.PKTABLE_NAME= v.ESCAPED_TABLE_NAME AND l.PKCOLUMN_NAME= v.ESCAPED_COLUMN_NAME)"
                + and("FKTABLE_CAT", "=", PUBLIC, " WHERE ")
                + and("FKTABLE_SCHEM", "=", PUBLIC) + and("FKTABLE_NAME", "=", table)
                + " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";
            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        try {
            if (table == null) {
                throw new InvalidParameterException("table", table);
            }
            table = normalizeName(table);
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;
            String nuS = unique ? "AND NON_UNIQUE IS FALSE" : "";
            String select = select("SYSTEM_INDEXINFO", List.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME),
                Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME))
                + from("SYSTEM_INDEXINFO", COLUMNS_VIEW)
                + on(List.of(TABLE_NAME, COLUMN_NAME), List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                + and(TABLE_CAT, "=", PUBLIC)
                + and(TABLE_SCHEM, "=", PUBLIC, " WHERE ")
                + and(TABLE_NAME, "=", table) + nuS;

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMajorVersion();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        try {
            return wrapped.getJDBCMinorVersion();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxBinaryLiteralLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        try {
            return wrapped.getMaxCatalogNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        try {
            return wrapped.getMaxCharLiteralLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        try {
            return wrapped.getMaxColumnNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInGroupBy();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        try {
            return wrapped.getMaxColumnsInIndex();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        try {
            return wrapped.getMaxColumnsInOrderBy();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        try {
            return wrapped.getMaxColumnsInSelect();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        try {
            return wrapped.getMaxColumnsInTable();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxConnections() throws SQLException {
        try {
            return wrapped.getMaxConnections();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        try {
            return wrapped.getMaxCursorNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        try {
            return wrapped.getMaxIndexLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        try {
            return wrapped.getMaxProcedureNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        try {
            return wrapped.getMaxRowSize();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        try {
            return wrapped.getMaxSchemaNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        try {
            return wrapped.getMaxStatementLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxStatements() throws SQLException {
        try {
            return wrapped.getMaxStatements();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        try {
            return wrapped.getMaxTableNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        try {
            return wrapped.getMaxTablesInSelect();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        try {
            return wrapped.getMaxUserNameLength();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        try {
            return wrapped.getNumericFunctions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    private boolean invokeWrapper(String catalog, String schema) {
        return connection.isShowSchema()
            && (catalog != null || schema != null) && (!PUBLIC.equals(catalog) || !PUBLIC.equals(schema));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (table == null) {
                throw new InvalidParameterException("table", table);
            }
            table = normalizeName(table);
            if (invokeWrapper(catalog, schema)) {
                return wrapped.getPrimaryKeys(catalog, schema, table);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;
            String select = select("SYSTEM_PRIMARYKEYS", List.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME),
                    Arrays.asList(cat, schem, TABLE_NAME, COLUMN_NAME))
                    + from("SYSTEM_PRIMARYKEYS", COLUMNS_VIEW)
                    + on(List.of(TABLE_NAME, COLUMN_NAME),
                            List.of(ESCAPED_TABLE_NAME, ESCAPED_COLUMN_NAME))
                    + and(TABLE_CAT, "=", PUBLIC, " WHERE ")
                    + and(TABLE_SCHEM, "=", PUBLIC) + and(TABLE_NAME, "=", table);
            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            columnNamePattern = normalizeName(columnNamePattern);
            return wrapped.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        try {
            return wrapped.getProcedures(catalog, schemaPattern, procedureNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        try {
            return wrapped.getProcedureTerm();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            return wrapped.getPseudoColumns(catalog, schemaPattern, tableNamePattern,
                    columnNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            return wrapped.getResultSetHoldability();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new FeatureNotSupportedRuntimeException("RowIdLifetime");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        if (connection.isShowSchema()) {
            try {
                return wrapped.getSchemas();
            } catch (SQLException _ex) {
                throw new UcanaccessSQLException(_ex);
            }
        }
        throw new FeatureNotSupportedRuntimeException();
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (connection.isShowSchema()) {
            try {
                return wrapped.getSchemas(catalog, schemaPattern);
            } catch (SQLException _ex) {
                throw new UcanaccessSQLException(_ex);
            }
        }
        throw new FeatureNotSupportedRuntimeException();
    }

    @Override
    public String getSchemaTerm() {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        try {
            return wrapped.getSearchStringEscape();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        try {
            return wrapped.getSQLKeywords();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getSQLStateType() throws SQLException {
        try {
            return wrapped.getSQLStateType();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getStringFunctions() throws SQLException {
        try {
            return wrapped.getStringFunctions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);
            return wrapped.getSuperTables(catalog, schemaPattern, tableNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        try {
            return wrapped.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        try {
            return wrapped.getSystemFunctions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);
            String select = select(TABLE_PRIVILEGES, List.of(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME),
                List.of(CAST_EXPR + " TABLE_CAT ", CAST_EXPR + " TABLE_SCHEM", TABLE_NAME))
                + from(TABLE_PRIVILEGES, TABLES)
                + on(List.of(TABLE_NAME), List.of(ESCAPED_TABLE_NAME))
                + and(TABLE_CATALOG, "=", PUBLIC, " WHERE ")
                + and("TABLE_SCHEMA", "=", PUBLIC)
                + and(TABLE_NAME, LIKE, tableNamePattern);

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try {
            tableNamePattern = normalizeName(tableNamePattern);

            if (invokeWrapper(catalog, schemaPattern)) {
                return wrapped.getTables(catalog, schemaPattern, tableNamePattern, types);
            }
            String cat = connection.isShowSchema() ? PUBLIC : null;
            String schem = connection.isShowSchema() ? PUBLIC : null;

            String select = select(SYSTEM_TABLES, List.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME),
                Arrays.asList(cat, schem, TABLE_NAME))
                + from(SYSTEM_TABLES, TABLES)
                + on(List.of(TABLE_NAME), List.of(ESCAPED_TABLE_NAME))
                + and(TABLE_CAT, "=", PUBLIC, " WHERE ")
                + and(TABLE_SCHEM, "=", PUBLIC)
                + and(TABLE_NAME, LIKE, tableNamePattern)
                + in(CUSTOM_ALIAS, "TYPE", types);

            return executeQuery(select);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            return wrapped.getTableTypes();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        try {
            return wrapped.getTimeDateFunctions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try {
            return wrapped.getTypeInfo();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        try {
            return wrapped.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getURL() {
        return connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        try {
            return wrapped.getUserName();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try {
            return wrapped.getVersionColumns(catalog, schema, table);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        try {
            return wrapped.insertsAreDetected(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        try {
            return wrapped.isCatalogAtStart();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            return wrapped.isReadOnly();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            return wrapped.isWrapperFor(iface);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        try {
            return wrapped.locatorsUpdateCopy();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    public static String normalizeName(String name) {
        if (name == null || name.isBlank()) {
            return name;
        }
        if (name.contains("%")) {
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
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtEnd();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        try {
            return wrapped.nullsAreSortedAtStart();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        try {
            return wrapped.nullsAreSortedHigh();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        try {
            return wrapped.nullsAreSortedLow();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersDeletesAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersInsertsAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.othersUpdatesAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownDeletesAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownInsertsAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        try {
            return wrapped.ownUpdatesAreVisible(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesLowerCaseQuotedIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesMixedCaseQuotedIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.storesUpperCaseQuotedIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithAddColumn();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        try {
            return wrapped.supportsAlterTableWithDropColumn();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92EntryLevelSQL();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92FullSQL();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        try {
            return wrapped.supportsANSI92IntermediateSQL();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        try {
            return wrapped.supportsBatchUpdates();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsCatalogsInDataManipulation();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInIndexDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInPrivilegeDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsCatalogsInProcedureCalls();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsCatalogsInTableDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        try {
            return wrapped.supportsColumnAliasing();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        try {
            return wrapped.supportsConvert();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        try {
            return wrapped.supportsConvert(fromType, toType);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsCoreSQLGrammar();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        try {
            return wrapped.supportsCorrelatedSubqueries();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        try {
            return wrapped.supportsDataDefinitionAndDataManipulationTransactions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        try {
            return wrapped.supportsDataManipulationTransactionsOnly();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsDifferentTableCorrelationNames();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        try {
            return wrapped.supportsExpressionsInOrderBy();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsExtendedSQLGrammar();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        try {
            return wrapped.supportsFullOuterJoins();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        try {
            return wrapped.supportsGetGeneratedKeys();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        try {
            return wrapped.supportsGroupBy();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        try {
            return wrapped.supportsGroupByBeyondSelect();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        try {
            return wrapped.supportsGroupByUnrelated();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        try {
            return wrapped.supportsIntegrityEnhancementFacility();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        try {
            return wrapped.supportsLikeEscapeClause();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        try {
            return wrapped.supportsLimitedOuterJoins();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        try {
            return wrapped.supportsMinimumSQLGrammar();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        try {
            return wrapped.supportsMixedCaseQuotedIdentifiers();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        try {
            return wrapped.supportsMultipleOpenResults();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        try {
            return wrapped.supportsMultipleResultSets();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        try {
            return wrapped.supportsMultipleTransactions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        try {
            return wrapped.supportsNamedParameters();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        try {
            return wrapped.supportsNonNullableColumns();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossCommit();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenCursorsAcrossRollback();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossCommit();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        try {
            return wrapped.supportsOpenStatementsAcrossRollback();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        try {
            return wrapped.supportsOrderByUnrelated();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        try {
            return wrapped.supportsOuterJoins();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        try {
            return wrapped.supportsPositionedDelete();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        try {
            return wrapped.supportsPositionedUpdate();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        try {
            return wrapped.supportsResultSetConcurrency(type, concurrency);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        try {
            return wrapped.supportsResultSetHoldability(holdability);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        try {
            return wrapped.supportsResultSetType(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        try {
            return wrapped.supportsSavepoints();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        try {
            return wrapped.supportsSchemasInDataManipulation();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInIndexDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInPrivilegeDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        try {
            return wrapped.supportsSchemasInProcedureCalls();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        try {
            return wrapped.supportsSchemasInTableDefinitions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        try {
            return wrapped.supportsSelectForUpdate();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        try {
            return wrapped.supportsStatementPooling();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        try {
            return wrapped.supportsStoredFunctionsUsingCallSyntax();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        try {
            return wrapped.supportsStoredProcedures();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInComparisons();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInExists();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInIns();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        try {
            return wrapped.supportsSubqueriesInQuantifieds();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        try {
            return wrapped.supportsTableCorrelationNames();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        try {
            return wrapped.supportsTransactionIsolationLevel(level);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        try {
            return wrapped.supportsTransactions();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        try {
            return wrapped.supportsUnion();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        try {
            return wrapped.supportsUnionAll();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        try {
            return wrapped.updatesAreDetected(type);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        try {
            return wrapped.usesLocalFilePerTable();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        try {
            return wrapped.usesLocalFiles();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }
}
