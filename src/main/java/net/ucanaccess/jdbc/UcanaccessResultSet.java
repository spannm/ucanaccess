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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import  org.hsqldb.jdbc.JDBCResultSet;

public class UcanaccessResultSet implements ResultSet {
	private ResultSet wrapped;
	private UcanaccessStatement wrappedStatement;
	
	public UcanaccessResultSet(ResultSet wrapped, UcanaccessStatement statement) {
		super();
		this.wrapped = wrapped;
		this.wrappedStatement = statement;
	}
	
	public boolean absolute(int row) throws SQLException {
		try {
			return wrapped.absolute(row);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void afterLast() throws SQLException {
		try {
			wrapped.afterLast();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void beforeFirst() throws SQLException {
		try {
			wrapped.beforeFirst();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void cancelRowUpdates() throws SQLException {
		try {
			wrapped.cancelRowUpdates();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void clearWarnings() throws SQLException {
		try {
			wrapped.clearWarnings();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void close() throws SQLException {
		try {
			wrapped.close();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void deleteRow() throws SQLException {
		try {
			wrapped.deleteRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int findColumn(String columnLabel) throws SQLException {
		try {
			return wrapped.findColumn(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean first() throws SQLException {
		try {
			return wrapped.first();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Array getArray(int idx) throws SQLException {
		try {
			return wrapped.getArray(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Array getArray(String columnLabel) throws SQLException {
		try {
			return wrapped.getArray(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getAsciiStream(int idx) throws SQLException {
		try {
			return wrapped.getAsciiStream(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		try {
			return wrapped.getAsciiStream(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public BigDecimal getBigDecimal(int idx) throws SQLException {
		try {
			return wrapped.getBigDecimal(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	@Deprecated
	public BigDecimal getBigDecimal(int idx, int arg1) throws SQLException {
		try {
			return wrapped.getBigDecimal(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		try {
			return wrapped.getBigDecimal(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	@Deprecated
	public BigDecimal getBigDecimal(String columnLabel, int arg1)
			throws SQLException {
		try {
			return wrapped.getBigDecimal(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getBinaryStream(int idx) throws SQLException {
		try {
			return wrapped.getBinaryStream(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		try {
			return wrapped.getBinaryStream(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Blob getBlob(int idx) throws SQLException {
		try {
			return wrapped.getBlob(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Blob getBlob(String columnLabel) throws SQLException {
		try {
			return wrapped.getBlob(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean getBoolean(int idx) throws SQLException {
		try {
			return wrapped.getBoolean(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean getBoolean(String columnLabel) throws SQLException {
		try {
			return wrapped.getBoolean(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public byte getByte(int idx) throws SQLException {
		try {
			return wrapped.getByte(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public byte getByte(String columnLabel) throws SQLException {
		try {
			return wrapped.getByte(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public byte[] getBytes(int idx) throws SQLException {
		try {
			return wrapped.getBytes(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public byte[] getBytes(String columnLabel) throws SQLException {
		try {
			return wrapped.getBytes(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Reader getCharacterStream(int idx) throws SQLException {
		try {
			return wrapped.getCharacterStream(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		try {
			return wrapped.getCharacterStream(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Clob getClob(int idx) throws SQLException {
		try {
			return wrapped.getClob(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Clob getClob(String columnLabel) throws SQLException {
		try {
			return wrapped.getClob(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getConcurrency() throws SQLException {
		try {
			return wrapped.getConcurrency();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getCursorName() throws SQLException {
		try {
			return wrapped.getCursorName();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Date getDate(int idx) throws SQLException {
		try {
			return wrapped.getDate(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Date getDate(int idx, Calendar arg1) throws SQLException {
		try {
			return wrapped.getDate(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Date getDate(String columnLabel) throws SQLException {
		try {
			return wrapped.getDate(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Date getDate(String columnLabel, Calendar arg1) throws SQLException {
		try {
			return wrapped.getDate(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public double getDouble(int idx) throws SQLException {
		try {
			return wrapped.getDouble(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public double getDouble(String columnLabel) throws SQLException {
		try {
			return wrapped.getDouble(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getFetchDirection() throws SQLException {
		try {
			return wrapped.getFetchDirection();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getFetchSize() throws SQLException {
		try {
			return wrapped.getFetchSize();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public float getFloat(int idx) throws SQLException {
		try {
			return wrapped.getFloat(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public float getFloat(String columnLabel) throws SQLException {
		try {
			return wrapped.getFloat(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getHoldability() throws SQLException {
		try {
			return wrapped.getHoldability();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getInt(int idx) throws SQLException {
		try {
			return wrapped.getInt(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getInt(String columnLabel) throws SQLException {
		try {
			return wrapped.getInt(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public long getLong(int idx) throws SQLException {
		try {
			return wrapped.getLong(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public long getLong(String columnLabel) throws SQLException {
		try {
			return wrapped.getLong(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public ResultSetMetaData getMetaData() throws SQLException {
		try {
			return wrapped.getMetaData();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Reader getNCharacterStream(int idx) throws SQLException {
		try {
			return wrapped.getNCharacterStream(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		try {
			return wrapped.getNCharacterStream(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public NClob getNClob(int idx) throws SQLException {
		try {
			return wrapped.getNClob(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public NClob getNClob(String columnLabel) throws SQLException {
		try {
			return wrapped.getNClob(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getNString(int idx) throws SQLException {
		try {
			return wrapped.getNString(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getNString(String columnLabel) throws SQLException {
		try {
			return wrapped.getNString(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Object getObject(int idx) throws SQLException {
		try {
			return wrapped.getObject(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		try {
			return ((JDBCResultSet)wrapped).getObject( columnIndex,  type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Object getObject(int idx, Map<String, Class<?>> arg1)
			throws SQLException {
		try {
			return wrapped.getObject(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Object getObject(String columnLabel) throws SQLException {
		try {
			return wrapped.getObject(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		try {
			return ((JDBCResultSet)wrapped).getObject( columnLabel,  type);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Object getObject(String columnLabel, Map<String, Class<?>> arg1)
			throws SQLException {
		try {
			return wrapped.getObject(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Ref getRef(int idx) throws SQLException {
		try {
			return wrapped.getRef(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Ref getRef(String columnLabel) throws SQLException {
		try {
			return wrapped.getRef(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getRow() throws SQLException {
		try {
			return wrapped.getRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public RowId getRowId(int idx) throws SQLException {
		try {
			return wrapped.getRowId(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public RowId getRowId(String columnLabel) throws SQLException {
		try {
			return wrapped.getRowId(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public short getShort(int idx) throws SQLException {
		try {
			return wrapped.getShort(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public short getShort(String columnLabel) throws SQLException {
		try {
			return wrapped.getShort(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public SQLXML getSQLXML(int idx) throws SQLException {
		try {
			return wrapped.getSQLXML(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		try {
			return wrapped.getSQLXML(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Statement getStatement() throws SQLException {
		try {
			return wrapped.getStatement();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getString(int idx) throws SQLException {
		try {
			return wrapped.getString(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getString(String columnLabel) throws SQLException {
		try {
			return wrapped.getString(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Time getTime(int idx) throws SQLException {
		try {
			return wrapped.getTime(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Time getTime(int idx, Calendar arg1) throws SQLException {
		try {
			return wrapped.getTime(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Time getTime(String columnLabel) throws SQLException {
		try {
			return wrapped.getTime(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Time getTime(String columnLabel, Calendar arg1) throws SQLException {
		try {
			return wrapped.getTime(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Timestamp getTimestamp(int idx) throws SQLException {
		try {
			return wrapped.getTimestamp(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Timestamp getTimestamp(int idx, Calendar arg1) throws SQLException {
		try {
			return wrapped.getTimestamp(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		try {
			return wrapped.getTimestamp(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Timestamp getTimestamp(String columnLabel, Calendar arg1)
			throws SQLException {
		try {
			return wrapped.getTimestamp(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int getType() throws SQLException {
		try {
			return wrapped.getType();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	@Deprecated
	public InputStream getUnicodeStream(int idx) throws SQLException {
		try {
			return wrapped.getUnicodeStream(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		try {
			return wrapped.getUnicodeStream(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public URL getURL(int idx) throws SQLException {
		try {
			Object obj = wrapped.getObject(idx);
			return getURL(obj);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	private URL getURL(Object obj) throws SQLException {
		try {
			if (obj instanceof String) {
				String s = (String) obj;
				if (s.startsWith("#") && s.endsWith("#")) {
					return new URL(s.substring(1, s.length() - 1));
				}
			}
			throw new SQLException("Invalid or unsupported url format");
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public URL getURL(String cn) throws SQLException {
		try {
			Object obj = wrapped.getObject(cn);
			return getURL(obj);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public SQLWarning getWarnings() throws SQLException {
		try {
			return wrapped.getWarnings();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public ResultSet getWrapped() {
		return wrapped;
	}
	
	public UcanaccessStatement getWrappedStatement() {
		return wrappedStatement;
	}
	
	public void insertRow() throws SQLException {
		try {
			wrapped.insertRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean isAfterLast() throws SQLException {
		try {
			return wrapped.isAfterLast();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean isBeforeFirst() throws SQLException {
		try {
			return wrapped.isBeforeFirst();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean isClosed() throws SQLException {
		try {
			return wrapped.isClosed();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean isFirst() throws SQLException {
		try {
			return wrapped.isFirst();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean isLast() throws SQLException {
		try {
			return wrapped.isLast();
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
	
	public boolean last() throws SQLException {
		try {
			return wrapped.last();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void moveToCurrentRow() throws SQLException {
		try {
			wrapped.moveToCurrentRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void moveToInsertRow() throws SQLException {
		try {
			wrapped.moveToInsertRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean next() throws SQLException {
		try {
			return wrapped.next();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean previous() throws SQLException {
		try {
			return wrapped.previous();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void refreshRow() throws SQLException {
		try {
			wrapped.refreshRow();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean relative(int idx) throws SQLException {
		try {
			return wrapped.relative(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean rowDeleted() throws SQLException {
		try {
			return wrapped.rowDeleted();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean rowInserted() throws SQLException {
		try {
			return wrapped.rowInserted();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean rowUpdated() throws SQLException {
		try {
			return wrapped.rowUpdated();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setFetchDirection(int idx) throws SQLException {
		try {
			wrapped.setFetchDirection(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setFetchSize(int idx) throws SQLException {
		try {
			wrapped.setFetchSize(idx);
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
	
	public void updateArray(int idx, Array arg1) throws SQLException {
		try {
			wrapped.updateArray(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateArray(String columnLabel, Array arg1) throws SQLException {
		try {
			wrapped.updateArray(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(int idx, InputStream arg1)
			throws SQLException {
		try {
			wrapped.updateAsciiStream(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(int idx, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateAsciiStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(int idx, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateAsciiStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(String columnLabel, InputStream arg1)
			throws SQLException {
		try {
			wrapped.updateAsciiStream(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(String columnLabel, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateAsciiStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateAsciiStream(String columnLabel, InputStream arg1,
			long arg2) throws SQLException {
		try {
			wrapped.updateAsciiStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBigDecimal(int idx, BigDecimal arg1) throws SQLException {
		try {
			wrapped.updateBigDecimal(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBigDecimal(String columnLabel, BigDecimal arg1)
			throws SQLException {
		try {
			wrapped.updateBigDecimal(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(int idx, InputStream arg1)
			throws SQLException {
		try {
			wrapped.updateBinaryStream(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(int idx, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateBinaryStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(int idx, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateBinaryStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(String columnLabel, InputStream arg1)
			throws SQLException {
		try {
			wrapped.updateBinaryStream(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(String columnLabel, InputStream arg1,
			int arg2) throws SQLException {
		try {
			wrapped.updateBinaryStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBinaryStream(String columnLabel, InputStream arg1,
			long arg2) throws SQLException {
		try {
			wrapped.updateBinaryStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(int idx, Blob arg1) throws SQLException {
		try {
			wrapped.updateBlob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(int idx, InputStream arg1) throws SQLException {
		try {
			wrapped.updateBlob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(int idx, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateBlob(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(String columnLabel, Blob arg1) throws SQLException {
		try {
			wrapped.updateBlob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(String columnLabel, InputStream arg1)
			throws SQLException {
		try {
			wrapped.updateBlob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBlob(String columnLabel, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateBlob(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBoolean(int idx, boolean arg1) throws SQLException {
		try {
			wrapped.updateBoolean(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBoolean(String columnLabel, boolean arg1)
			throws SQLException {
		try {
			wrapped.updateBoolean(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateByte(int idx, byte arg1) throws SQLException {
		try {
			wrapped.updateByte(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateByte(String columnLabel, byte arg1) throws SQLException {
		try {
			wrapped.updateByte(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBytes(int idx, byte[] arg1) throws SQLException {
		try {
			wrapped.updateBytes(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateBytes(String columnLabel, byte[] arg1)
			throws SQLException {
		try {
			wrapped.updateBytes(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(int idx, Reader arg1) throws SQLException {
		try {
			wrapped.updateCharacterStream(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(int idx, Reader arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateCharacterStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(int idx, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateCharacterStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(String columnLabel, Reader arg1)
			throws SQLException {
		try {
			wrapped.updateCharacterStream(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(String columnLabel, Reader arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateCharacterStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateCharacterStream(String columnLabel, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateCharacterStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(int idx, Clob arg1) throws SQLException {
		try {
			wrapped.updateClob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(int idx, Reader arg1) throws SQLException {
		try {
			wrapped.updateClob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(int idx, Reader arg1, long arg2) throws SQLException {
		try {
			wrapped.updateClob(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(String columnLabel, Clob arg1) throws SQLException {
		try {
			wrapped.updateClob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(String columnLabel, Reader arg1) throws SQLException {
		try {
			wrapped.updateClob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateClob(String columnLabel, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateClob(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateDate(int idx, Date arg1) throws SQLException {
		try {
			wrapped.updateDate(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateDate(String columnLabel, Date arg1) throws SQLException {
		try {
			wrapped.updateDate(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateDouble(int idx, double arg1) throws SQLException {
		try {
			wrapped.updateDouble(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateDouble(String columnLabel, double arg1)
			throws SQLException {
		try {
			wrapped.updateDouble(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateFloat(int idx, float arg1) throws SQLException {
		try {
			wrapped.updateFloat(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateFloat(String columnLabel, float arg1) throws SQLException {
		try {
			wrapped.updateFloat(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateInt(int idx, int arg1) throws SQLException {
		try {
			wrapped.updateInt(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateInt(String columnLabel, int arg1) throws SQLException {
		try {
			wrapped.updateInt(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateLong(int idx, long arg1) throws SQLException {
		try {
			wrapped.updateLong(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateLong(String columnLabel, long arg1) throws SQLException {
		try {
			wrapped.updateLong(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNCharacterStream(int idx, Reader arg1)
			throws SQLException {
		try {
			wrapped.updateNCharacterStream(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNCharacterStream(int idx, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateNCharacterStream(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNCharacterStream(String columnLabel, Reader arg1)
			throws SQLException {
		try {
			wrapped.updateNCharacterStream(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNCharacterStream(String columnLabel, Reader arg1,
			long arg2) throws SQLException {
		try {
			wrapped.updateNCharacterStream(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(int idx, NClob arg1) throws SQLException {
		try {
			wrapped.updateNClob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(int idx, Reader arg1) throws SQLException {
		try {
			wrapped.updateNClob(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(int idx, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateNClob(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(String columnLabel, NClob arg1) throws SQLException {
		try {
			wrapped.updateNClob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(String columnLabel, Reader arg1)
			throws SQLException {
		try {
			wrapped.updateNClob(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNClob(String columnLabel, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.updateNClob(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNString(int idx, String arg1) throws SQLException {
		try {
			wrapped.updateNString(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNString(String columnLabel, String arg1)
			throws SQLException {
		try {
			wrapped.updateNString(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNull(int idx) throws SQLException {
		try {
			wrapped.updateNull(idx);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateNull(String columnLabel) throws SQLException {
		try {
			wrapped.updateNull(columnLabel);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateObject(int idx, Object arg1) throws SQLException {
		try {
			wrapped.updateObject(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateObject(int idx, Object arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateObject(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateObject(String columnLabel, Object arg1)
			throws SQLException {
		try {
			wrapped.updateObject(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateObject(String columnLabel, Object arg1, int arg2)
			throws SQLException {
		try {
			wrapped.updateObject(columnLabel, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateRef(int idx, Ref arg1) throws SQLException {
		try {
			wrapped.updateRef(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateRef(String columnLabel, Ref arg1) throws SQLException {
		try {
			wrapped.updateRef(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateRow() throws SQLException {
		try {
			new UpdateResultSet(this).execute();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateRowId(int idx, RowId arg1) throws SQLException {
		try {
			wrapped.updateRowId(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateRowId(String columnLabel, RowId arg1) throws SQLException {
		try {
			wrapped.updateRowId(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateShort(int idx, short arg1) throws SQLException {
		try {
			wrapped.updateShort(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateShort(String columnLabel, short arg1) throws SQLException {
		try {
			wrapped.updateShort(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateSQLXML(int idx, SQLXML arg1) throws SQLException {
		try {
			wrapped.updateSQLXML(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateSQLXML(String columnLabel, SQLXML arg1)
			throws SQLException {
		try {
			wrapped.updateSQLXML(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateString(int idx, String arg1) throws SQLException {
		try {
			wrapped.updateString(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateString(String columnLabel, String arg1)
			throws SQLException {
		try {
			wrapped.updateString(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateTime(int idx, Time arg1) throws SQLException {
		try {
			wrapped.updateTime(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateTime(String columnLabel, Time arg1) throws SQLException {
		try {
			wrapped.updateTime(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void updateTimestamp(int idx, Timestamp arg1) throws SQLException {
		try {
			wrapped.updateTimestamp(idx, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}


	public void updateTimestamp(String columnLabel, Timestamp arg1)
			throws SQLException {
		try {
			wrapped.updateTimestamp(columnLabel, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean wasNull() throws SQLException {
		try {
			return wrapped.wasNull();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
}
