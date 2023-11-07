package net.ucanaccess.converters;

import static net.ucanaccess.util.SqlConstants.*;

import net.ucanaccess.util.Try;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Metadata {

    private static final String     SCHEMA                    = "CREATE SCHEMA UCA_METADATA AUTHORIZATION DBA";

    private static final String     TABLES                    =
        "CREATE TABLE UCA_METADATA.TABLES(TABLE_ID INTEGER IDENTITY, TABLE_NAME LONGVARCHAR, ESCAPED_TABLE_NAME LONGVARCHAR, TYPE VARCHAR(5),UNIQUE(TABLE_NAME)) ";
    private static final String     COLUMNS                   = "CREATE MEMORY TABLE "
        + "UCA_METADATA.COLUMNS(COLUMN_ID INTEGER IDENTITY, COLUMN_NAME LONGVARCHAR,ESCAPED_COLUMN_NAME LONGVARCHAR, "
        + "ORIGINAL_TYPE VARCHAR(20), COLUMN_DEF LONGVARCHAR,IS_GENERATEDCOLUMN VARCHAR(3),TABLE_ID INTEGER, UNIQUE(TABLE_ID,COLUMN_NAME) )";

    private static final String     PROP                      =
        "CREATE MEMORY TABLE UCA_METADATA.PROP(NAME LONGVARCHAR PRIMARY KEY, MAX_LEN INTEGER, DEFAULT_VALUE VARCHAR(20), DESCRIPTION LONGVARCHAR)";

    private static final Object[][] PROP_DATA       = new Object[][] {
            {"newdatabaseversion", 8, null},
            {"jackcessopener", 500, null},
            {"password", 500, null},
            {"memory", 10, "true"},
            {"lobscale", 2, "2"},
            {"keepmirror", 500, "2"},
            {"showschema", 10, "false"},
            {"inactivitytimeout", 10, "2"},
            {"singleconnection", 10, "false"},
            {"immediatelyreleaseresources", 10, "false"},
            {"lockmdb", 10, "false"},
            {"openexclusive", 500, "false"},
            {"remap", 500, null},
            {"columnorder", 10, "data"},
            {"mirrorfolder", 500, null},
            {"ignorecase", 10, "true"},
            {"sysschema", 10, "false"},
            {"skipindexes", 10, "false"},
            {"preventreloading", 10, "false"},
            {"concatnulls", 10, "false"}
    };

    private static final String     COLUMNS_VIEW              = "CREATE VIEW   UCA_METADATA.COLUMNS_VIEW as "
        + "SELECT t.TABLE_NAME, c.COLUMN_NAME,t.ESCAPED_TABLE_NAME, c.ESCAPED_COLUMN_NAME,c.COLUMN_DEF,c.IS_GENERATEDCOLUMN,"
        + "CASE WHEN(c.ORIGINAL_TYPE IN ('COUNTER' ,'GUID')) THEN 'YES' ELSE 'NO' END as IS_AUTOINCREMENT,c.ORIGINAL_TYPE "
        + "FROM UCA_METADATA.COLUMNS c INNER JOIN UCA_METADATA.TABLES t ON (t.TABLE_ID=c.TABLE_ID)";

    private static final String     FK                        = "ALTER TABLE UCA_METADATA.COLUMNS   "
        + "ADD CONSTRAINT UCA_METADATA_FK FOREIGN KEY (TABLE_ID) REFERENCES UCA_METADATA.TABLES (TABLE_ID) ON DELETE CASCADE";

    private static final String     TABLE_RECORD              =
        "INSERT INTO UCA_METADATA.TABLES( TABLE_NAME,ESCAPED_TABLE_NAME, TYPE) VALUES(?,?,?)";
    private static final String     COLUMN_RECORD             =
        "INSERT INTO UCA_METADATA.COLUMNS(COLUMN_NAME,ESCAPED_COLUMN_NAME,ORIGINAL_TYPE, IS_GENERATEDCOLUMN,TABLE_ID) "
            + "VALUES(?,?,?,'NO',?)";

    private static final String     SELECT_COLUMN             =
        "SELECT DISTINCT c.COLUMN_NAME,c.ORIGINAL_TYPE IN('COUNTER', 'GUID') as IS_AUTOINCREMENT, c.ORIGINAL_TYPE='MONEY' as IS_CURRENCY "
            + "FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES t "
            + "ON(t.TABLE_ID=c.TABLE_ID ) WHERE t.ESCAPED_TABLE_NAME=nvl(?,t.ESCAPED_TABLE_NAME) AND c.ESCAPED_COLUMN_NAME=? ";

    private static final String     SELECT_COLUMN_ESCAPED     = "SELECT c.ESCAPED_COLUMN_NAME "
        + "FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES t "
        + "ON(t.TABLE_ID=c.TABLE_ID ) WHERE t.TABLE_NAME=nvl(?,t.TABLE_NAME) AND c.COLUMN_NAME=?";

    private static final String     SELECT_TABLE_ESCAPED      =
        "SELECT ESCAPED_TABLE_NAME FROM UCA_METADATA.TABLES WHERE TABLE_NAME=?";

    private static final String     SELECT_TABLE_METADATA     =
        "SELECT TABLE_ID, TABLE_NAME FROM UCA_METADATA.TABLES WHERE ESCAPED_TABLE_NAME=? ";
    private static final String     DROP_TABLE                = "DELETE FROM UCA_METADATA.TABLES WHERE TABLE_NAME=?";
    private static final String     UPDATE_COLUMN_DEF         =
        "UPDATE UCA_METADATA.COLUMNS c SET c.COLUMN_DEF=? WHERE COLUMN_NAME=? "
            + " AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID) ";
    private static final String     UPDATE_IS_GENERATEDCOLUMN =
        "UPDATE UCA_METADATA.COLUMNS c SET c.IS_GENERATEDCOLUMN='YES' WHERE COLUMN_NAME=? "
            + " AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID) ";

    private static final String     SELECT_COLUMNS            =
        "SELECT DISTINCT c.COLUMN_NAME,c.ORIGINAL_TYPE IN('COUNTER', 'GUID') as IS_AUTOINCREMENT, c.ORIGINAL_TYPE='MONEY' as IS_CURRENCY "
            + "FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES t "
            + "ON(t.TABLE_ID=c.TABLE_ID ) WHERE t.ESCAPED_TABLE_NAME=nvl(?,t.ESCAPED_TABLE_NAME) ";
    private static final String     RENAME                    =
        "UPDATE UCA_METADATA.TABLES SET TABLE_NAME=?,ESCAPED_TABLE_NAME=? WHERE TABLE_NAME=?";

    private Connection              conn;

    public enum Types {
        VIEW,
        TABLE
    }

    public Metadata(Connection _conn) {
        conn = _conn;

    }

    public void createMetadata() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(SCHEMA);
            st.execute(PROP);
            st.execute(TABLES);
            st.execute(COLUMNS);
            st.execute(FK);
            st.execute(COLUMNS_VIEW);
            loadProp();
        }
    }

    public void loadProp() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO UCA_METADATA.PROP(NAME, MAX_LEN, DEFAULT_VALUE, DESCRIPTION) VALUES(?, ?, ?, ?)")) {
            for (Object[] ob : PROP_DATA) {
                ps.setObject(1, ob[0]);
                ps.setObject(2, ob[1]);
                ps.setObject(3, ob[2]);
                ps.setObject(4, "see ucanaccess website");
                ps.execute();
            }

        }

    }

    public Integer newTable(String name, String escaped, Types type) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(TABLE_RECORD, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.setString(2, escaped);
            ps.setString(3, type.name());
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            rs.next();

            return rs.getInt(1);
        } catch (SQLException _ex) {
            return getTableId(escaped);
        }
    }

    public void newColumn(String name, String escaped, String originalType, Integer idTable) {
        if (idTable < 0) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(COLUMN_RECORD)) {
            ps.setString(1, name);
            ps.setString(2, escaped);
            ps.setString(3, originalType);
            ps.setInt(4, idTable);
            ps.executeUpdate();
        } catch (SQLException ignored) {

        }
    }

    public List<String> getColumnNames(String _tableName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMNS), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_tableName) ? null : _tableName);
            ResultSet rs = ps.executeQuery();
            List<String> result = new ArrayList<>();
            while (rs.next()) {
                result.add(rs.getString(COLUMN_NAME));
            }
            return !SYSTEM_SUBQUERY.equals(_tableName) ? result : null;
        }).orThrow();
    }

    public String getColumnName(String _escapedTableName, String _escapedColumnName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMN), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_escapedTableName) ? null : _escapedTableName);
            ps.setString(2, _escapedColumnName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String res = rs.getString(COLUMN_NAME);
                if (!SYSTEM_SUBQUERY.equals(_escapedTableName) || !rs.next()) {
                    return res;
                }
            }
            return null;
        }).orThrow();
    }

    public String getEscapedColumnName(String _tableName, String _columnName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMN_ESCAPED), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_tableName) ? null : _tableName);
            ps.setString(2, _columnName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getString(ESCAPED_COLUMN_NAME) : null;
        }).orThrow();
    }

    public String getEscapedTableName(String tableName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_TABLE_ESCAPED), ps -> {
            ps.setString(1, tableName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getString(ESCAPED_TABLE_NAME) : null;
        }).orThrow();
    }

    public boolean isAutoIncrement(String _tableName, String columnName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMN), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_tableName) ? null : _tableName);
            ps.setString(2, columnName);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getBoolean(IS_AUTOINCREMENT);
        }).orThrow();
    }

    public boolean isCurrency(String _tableName, String _columnName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMN), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_tableName) ? null : _tableName);
            ps.setString(2, _columnName);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getBoolean(IS_CURRENCY);
        }).orThrow();
    }

    public Integer getTableId(String _escapedName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_TABLE_METADATA), ps -> {
            ps.setString(1, _escapedName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getInt(TABLE_ID) : -1;
        }).orThrow();
    }

    public String getTableName(String _escapedName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_TABLE_METADATA), ps -> {
            ps.setString(1, _escapedName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getString(TABLE_NAME) : null;
        }).orThrow();
    }

    public void dropTable(String _tableName) throws SQLException {
        Try.withResources(() -> conn.prepareStatement(DROP_TABLE), ps -> {
            ps.setString(1, _tableName);
            ps.execute();
        }).orThrow();
    }

    public void columnDef(String _tableName, String _columnName, String _def) throws SQLException {
        Try.withResources(() -> conn.prepareStatement(UPDATE_COLUMN_DEF), ps -> {
            ps.setString(1, _def);
            ps.setString(2, _columnName);
            ps.setString(3, _tableName);
            ps.execute();
        }).orThrow();
    }

    public void calculatedField(String _tableName, String _columnName) throws SQLException {
        Try.withResources(() -> conn.prepareStatement(UPDATE_IS_GENERATEDCOLUMN), ps -> {
            ps.setString(1, _columnName);
            ps.setString(2, _tableName);
            ps.execute();
        }).orThrow();
    }

    public void rename(String _oldTableName, String _newTableName, String _newEscapedTableName) throws SQLException {
        Try.withResources(() -> conn.prepareStatement(RENAME), ps -> {
            ps.setString(1, _newTableName);
            ps.setString(2, _newEscapedTableName);
            ps.setString(3, _oldTableName);
            ps.executeUpdate();
        }).orThrow();
    }

}
