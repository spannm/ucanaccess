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
*/
package net.ucanaccess.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.SQLConverter;

public class UcanaccessResultSetMetaData implements ResultSetMetaData {

    private ResultSetMetaData wrapped;
    private Metadata          metadata;

    private Map<String, String> aliases;

    public UcanaccessResultSetMetaData(ResultSetMetaData _wrapped, Map<String, String> _aliases,
            UcanaccessResultSet _resultSet) throws SQLException {
        this.wrapped = _wrapped;

        this.metadata = new Metadata(_resultSet.getStatement().getConnection());
        this.aliases = _aliases;
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
        return this.aliases.containsKey(name) ? this.aliases.get(name) : getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        String cn = this.metadata.getColumnName(tableName, columnName);
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
        if (Metadata.SYSTEM_SUBQUERY.equals(tableName)) {
            return tableName;
        }
        return this.metadata.getTableName(tableName);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        return this.metadata.isAutoIncrement(tableName, columnName);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return wrapped.isCaseSensitive(column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        String columnName = SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
        String tableName = SQLConverter.preEscapingIdentifier(wrapped.getTableName(column));
        return this.metadata.isCurrency(tableName, columnName);
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
