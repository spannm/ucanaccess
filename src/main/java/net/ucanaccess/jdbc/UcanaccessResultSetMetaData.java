package net.ucanaccess.jdbc;

import static net.ucanaccess.util.SqlConstants.SYSTEM_SUBQUERY;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.SQLConverter;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class UcanaccessResultSetMetaData implements ResultSetMetaData {

    private ResultSetMetaData         wrapped;
    private Metadata                  metadata;

    private final Map<String, String> aliases;

    public UcanaccessResultSetMetaData(ResultSetMetaData _wrapped, Map<String, String> _aliases,
            UcanaccessResultSet _resultSet) throws SQLException {
        wrapped = _wrapped;

        metadata = new Metadata(_resultSet.getStatement().getConnection());
        aliases = _aliases;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return wrapped.getCatalogName(column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return wrapped.getColumnClassName(column);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return wrapped.getColumnCount();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return wrapped.getColumnDisplaySize(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        String name = SQLConverter.preEscapingIdentifier(wrapped.getColumnLabel(column));
        return aliases.containsKey(name) ? aliases.get(name) : getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        String cn = metadata.getColumnName(tableName, columnName);
        return cn == null ? columnName : cn;

    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return wrapped.getColumnType(column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return wrapped.getColumnTypeName(column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return wrapped.getPrecision(column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return wrapped.getScale(column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return wrapped.getSchemaName(column);
    }

    @Override
    public String getTableName(int column) throws SQLException {

        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        if (SYSTEM_SUBQUERY.equals(tableName)) {
            return tableName;
        }
        return metadata.getTableName(tableName);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        return metadata.isAutoIncrement(tableName, columnName);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return wrapped.isCaseSensitive(column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        return metadata.isCurrency(tableName, columnName);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return wrapped.isDefinitelyWritable(column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return wrapped.isNullable(column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return wrapped.isReadOnly(column);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return wrapped.isSearchable(column);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return wrapped.isSigned(column);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrapped.isWrapperFor(iface);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return wrapped.isWritable(column);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrapped.unwrap(iface);
    }

}
