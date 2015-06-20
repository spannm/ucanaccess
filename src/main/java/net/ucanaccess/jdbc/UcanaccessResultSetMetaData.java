/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.SQLConverter;

public class UcanaccessResultSetMetaData implements ResultSetMetaData {
	
	private ResultSetMetaData wrapped;
	private Metadata metadata;
	
	private Map<String,String> aliases;
	public  UcanaccessResultSetMetaData(ResultSetMetaData wrapped,
			Map<String,String> aliases,UcanaccessResultSet resultSet) throws SQLException {
		super();
		this.wrapped = wrapped;
		
		this.metadata=new Metadata(resultSet.getStatement().getConnection());
		this.aliases=aliases;
	}
	public String getCatalogName(int column) throws SQLException {
		return wrapped.getCatalogName(column);
	}
	public String getColumnClassName(int column) throws SQLException {
		return wrapped.getColumnClassName(column);
	}
	public int getColumnCount() throws SQLException {
		return wrapped.getColumnCount();
	}
	public int getColumnDisplaySize(int column) throws SQLException {
		return wrapped.getColumnDisplaySize(column);
	}
	public String getColumnLabel(int column) throws SQLException {
		String name=SQLConverter.preEscapingIdentifier(wrapped.getColumnLabel(column));
		return this.aliases.containsKey(name)?this.aliases.get(name):getColumnName(column);
	}
	
	public String getColumnName(int column) throws SQLException {
		String columnName= SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
		String tableName=SQLConverter.preEscapingIdentifier( wrapped.getTableName(column));
		String cn= this.metadata.getColumn(tableName, columnName);
		return cn==null?columnName:cn;
		
	}
	public int getColumnType(int column) throws SQLException {
		return wrapped.getColumnType(column);
	}
	public String getColumnTypeName(int column) throws SQLException {
		return wrapped.getColumnTypeName(column);
	}
	public int getPrecision(int column) throws SQLException {
		return wrapped.getPrecision(column);
	}
	public int getScale(int column) throws SQLException {
		return wrapped.getScale(column);
	}
	public String getSchemaName(int column) throws SQLException {
		return wrapped.getSchemaName(column);
	}
	public String getTableName(int column) throws SQLException {
		return wrapped.getTableName(column);
	}
	public boolean isAutoIncrement(int column) throws SQLException {
		String columnName= SQLConverter.preEscapingIdentifier(wrapped.getColumnName(column));
		String tableName=SQLConverter.preEscapingIdentifier( wrapped.getTableName(column));
		return this.metadata.isAutoIncrement(tableName, columnName);
	}
	public boolean isCaseSensitive(int column) throws SQLException {
		return wrapped.isCaseSensitive(column);
	}
	public boolean isCurrency(int column) throws SQLException {
		return wrapped.isCurrency(column);
	}
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return wrapped.isDefinitelyWritable(column);
	}
	public int isNullable(int column) throws SQLException {
		return wrapped.isNullable(column);
	}
	public boolean isReadOnly(int column) throws SQLException {
		return wrapped.isReadOnly(column);
	}
	public boolean isSearchable(int column) throws SQLException {
		return wrapped.isSearchable(column);
	}
	public boolean isSigned(int column) throws SQLException {
		return wrapped.isSigned(column);
	}
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return wrapped.isWrapperFor(iface);
	}
	public boolean isWritable(int column) throws SQLException {
		return wrapped.isWritable(column);
	}
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return wrapped.unwrap(iface);
	}
	
	
}
