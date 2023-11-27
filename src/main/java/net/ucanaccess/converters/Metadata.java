package net.ucanaccess.converters;

import static net.ucanaccess.type.SqlConstants.*;

import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.type.ColumnOrder;
import net.ucanaccess.type.ObjectType;
import net.ucanaccess.util.Try;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Metadata {

    private static final String SCHEMA  = "CREATE SCHEMA UCA_METADATA AUTHORIZATION DBA";

    private static final String TABLES  =
        "CREATE TABLE UCA_METADATA.TABLES(TABLE_ID INTEGER IDENTITY, TABLE_NAME LONGVARCHAR, ESCAPED_TABLE_NAME LONGVARCHAR, TYPE VARCHAR(5), UNIQUE(TABLE_NAME))";
    private static final String COLUMNS =
        "CREATE MEMORY TABLE UCA_METADATA.COLUMNS(COLUMN_ID INTEGER IDENTITY, COLUMN_NAME LONGVARCHAR, ESCAPED_COLUMN_NAME LONGVARCHAR, "
            + "ORIGINAL_TYPE VARCHAR(20), COLUMN_DEF LONGVARCHAR, IS_GENERATEDCOLUMN VARCHAR(3), TABLE_ID INTEGER, UNIQUE(TABLE_ID, COLUMN_NAME) )";

    private static final String PROP    =
        "CREATE MEMORY TABLE UCA_METADATA.PROP(NAME LONGVARCHAR PRIMARY KEY, MAX_LEN INTEGER, DEFAULT_VALUE VARCHAR(20), DESCRIPTION LONGVARCHAR)";

    /**
     * Enum of Ucanaccess driver properties.
     *
     * @author Markus Spann
     * @since v5.1.0
     */
    public enum Property {
        user(String.class, null, 500),
        password(String.class, null, 500),

        columnOrder(ColumnOrder.class, ColumnOrder.DATA, 10),
        concatNulls(Boolean.class, false, 10),
        encrypt(Boolean.class, false, 10),
        ignoreCase(Boolean.class, true, 10),
        immediatelyReleaseResources(Boolean.class, false, 10),
        inactivityTimeout(Integer.class, 2, 10),
        jackcessOpener(String.class, null, 500),
        keepMirror(String.class, "2", 500),
        lobScale(Integer.class, 2, 2, 1, 2, 4, 8, 16, 32),
        lockMdb(Boolean.class, false, 10),
        memory(Boolean.class, true, 10),
        mirrorFolder(String.class, null, 500),
        newDatabaseVersion(AccessVersion.class, null, 8),
        openExclusive(Boolean.class, false, 500),
        preventReloading(Boolean.class, false, 10),
        reMap(String.class, null, 500),
        readOnlyMirror(Boolean.class, false, 10),
        showSchema(Boolean.class, false, 10),
        singleConnection(Boolean.class, false, 10),
        skipIndexes(Boolean.class, false, 10),
        supportsAccessLike(Boolean.class, false, 10),
        sysSchema(Boolean.class, false, 10);

        private final Class<?>     type;
        private final int          maxLen;
        private final Object       defaultValue;
        private final List<Object> validValues;
        private final String       description;

        <T> Property(Class<T> _type, T _defaultValue, int _maxLen, Object... _validValues) {
            maxLen = _maxLen;
            type = _type;
            defaultValue = _defaultValue;
            description = "see ucanaccess website";
            validValues = Arrays.asList(_validValues);
        }

        public Class<?> getType() {
            return type;
        }

        public int getMaxLen() {
            return maxLen;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public String getDescription() {
            return description;
        }

        public boolean isValidValue(Object _val) {
            if (_val == null) {
                return false;
            } else if (type.isEnum()) {
                for (Object en : type.getEnumConstants()) {
                    if (en == _val || en.toString().equalsIgnoreCase(_val.toString())) {
                        return true;
                    }
                }
                return false;
            } else if (validValues.isEmpty()) {
                if (type == Boolean.class) {
                    return "true".equalsIgnoreCase(_val.toString()) || "false".equalsIgnoreCase(_val.toString());
                } else if (type == Integer.class) {
                    return Try.catching(() -> Integer.parseInt(_val.toString())).map(i -> true).orElse(false);
                }
                return true;
            } else if (_val instanceof CharSequence) {
                return validValues.contains(_val.toString().toUpperCase());
            } else {
                return validValues.contains(_val);
            }
        }

        public static Property parse(Object _val) {
            if (_val == null) {
                return null;
            }
            String val = _val.toString().strip().toLowerCase();
            for (Property prop : values()) {
                if (val.equalsIgnoreCase(prop.name())) {
                    return prop;
                }
            }
            return null;
        }

    }

    private static final String COLUMNS_VIEW              =
        "CREATE VIEW UCA_METADATA.COLUMNS_VIEW AS "
            + "SELECT t.TABLE_NAME, c.COLUMN_NAME,t.ESCAPED_TABLE_NAME, c.ESCAPED_COLUMN_NAME, c.COLUMN_DEF, c.IS_GENERATEDCOLUMN, "
            + "CASE WHEN(c.ORIGINAL_TYPE IN ('COUNTER' ,'GUID')) THEN 'YES' ELSE 'NO' END as IS_AUTOINCREMENT, c.ORIGINAL_TYPE "
            + "FROM UCA_METADATA.COLUMNS c INNER JOIN UCA_METADATA.TABLES t ON (t.TABLE_ID = c.TABLE_ID)";

    private static final String FK                        =
        "ALTER TABLE UCA_METADATA.COLUMNS "
            + "ADD CONSTRAINT UCA_METADATA_FK FOREIGN KEY (TABLE_ID) REFERENCES UCA_METADATA.TABLES (TABLE_ID) ON DELETE CASCADE";

    private static final String TABLE_RECORD              =
        "INSERT INTO UCA_METADATA.TABLES( TABLE_NAME,ESCAPED_TABLE_NAME, TYPE) VALUES(?,?,?)";
    private static final String COLUMN_RECORD             =
        "INSERT INTO UCA_METADATA.COLUMNS(COLUMN_NAME,ESCAPED_COLUMN_NAME,ORIGINAL_TYPE, IS_GENERATEDCOLUMN,TABLE_ID) "
            + "VALUES(?,?,?,'NO',?)";

    private static final String SELECT_COLUMN             =
        "SELECT DISTINCT c.COLUMN_NAME,c.ORIGINAL_TYPE IN('COUNTER', 'GUID') as IS_AUTOINCREMENT, c.ORIGINAL_TYPE='MONEY' as IS_CURRENCY "
            + "FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES t "
            + "ON (t.TABLE_ID = c.TABLE_ID ) WHERE t.ESCAPED_TABLE_NAME=nvl(?,t.ESCAPED_TABLE_NAME) AND c.ESCAPED_COLUMN_NAME=? ";

    private static final String SELECT_COLUMN_ESCAPED     =
        "SELECT c.ESCAPED_COLUMN_NAME "
            + "FROM UCA_METADATA.COLUMNS c INNER JOIN UCA_METADATA.TABLES t "
            + "ON (t.TABLE_ID = c.TABLE_ID) WHERE t.TABLE_NAME=nvl(?,t.TABLE_NAME) AND c.COLUMN_NAME=?";

    private static final String SELECT_TABLE_ESCAPED      =
        "SELECT ESCAPED_TABLE_NAME FROM UCA_METADATA.TABLES WHERE TABLE_NAME = ?";

    private static final String SELECT_TABLE_METADATA     =
        "SELECT TABLE_ID, TABLE_NAME FROM UCA_METADATA.TABLES WHERE ESCAPED_TABLE_NAME = ?";
    private static final String DROP_TABLE                = "DELETE FROM UCA_METADATA.TABLES WHERE TABLE_NAME = ?";
    private static final String UPDATE_COLUMN_DEF         =
        "UPDATE UCA_METADATA.COLUMNS c SET c.COLUMN_DEF=? WHERE COLUMN_NAME=? "
            + " AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID)";
    private static final String UPDATE_IS_GENERATEDCOLUMN =
        "UPDATE UCA_METADATA.COLUMNS c SET c.IS_GENERATEDCOLUMN='YES' WHERE COLUMN_NAME=? "
            + " AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID)";

    private static final String SELECT_COLUMNS            =
        "SELECT DISTINCT c.COLUMN_NAME,c.ORIGINAL_TYPE IN('COUNTER', 'GUID') as IS_AUTOINCREMENT, c.ORIGINAL_TYPE='MONEY' as IS_CURRENCY "
            + "FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES t "
            + "ON (t.TABLE_ID = c.TABLE_ID ) WHERE t.ESCAPED_TABLE_NAME = nvl(?, t.ESCAPED_TABLE_NAME)";
    private static final String RENAME                    =
        "UPDATE UCA_METADATA.TABLES SET TABLE_NAME=?, ESCAPED_TABLE_NAME=? WHERE TABLE_NAME=?";

    private Connection          conn;

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
            for (Property prop : Property.values()) {
                ps.setObject(1, prop.name());
                ps.setObject(2, prop.getMaxLen());
                ps.setObject(3, Optional.ofNullable(prop.getDefaultValue()).map(Object::toString).orElse(null));
                ps.setObject(4, prop.getDescription());
                ps.execute();
            }
        }
    }

    public Integer newTable(String _name, String _escaped, ObjectType _type) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(TABLE_RECORD, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, _name);
            ps.setString(2, _escaped);
            ps.setString(3, _type.name());
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            rs.next();

            return rs.getInt(1);
        } catch (SQLException _ex) {
            return getTableId(_escaped);
        }
    }

    public void newColumn(String _name, String _escaped, String _originalType, Integer _idTable) {
        if (_idTable < 0) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(COLUMN_RECORD)) {
            ps.setString(1, _name);
            ps.setString(2, _escaped);
            ps.setString(3, _originalType);
            ps.setInt(4, _idTable);
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

    public String getEscapedTableName(String _tableName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_TABLE_ESCAPED), ps -> {
            ps.setString(1, _tableName);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getString(ESCAPED_TABLE_NAME) : null;
        }).orThrow();
    }

    public boolean isAutoIncrement(String _tableName, String _columnName) throws SQLException {
        return Try.withResources(() -> conn.prepareStatement(SELECT_COLUMN), ps -> {
            ps.setString(1, SYSTEM_SUBQUERY.equals(_tableName) ? null : _tableName);
            ps.setString(2, _columnName);
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
