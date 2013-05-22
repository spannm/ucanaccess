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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class UcanaccessPreparedStatement extends UcanaccessStatement implements
		PreparedStatement {
	private Map<Integer, Blob> blobMap = new HashMap<Integer, Blob>();
	private PreparedStatement wrapped;
	
	public UcanaccessPreparedStatement(PreparedStatement hidden,
			UcanaccessConnection connection) throws SQLException {
		super(hidden, connection);
		this.wrapped = hidden;
	}
	
	public void addBatch() throws SQLException {
		try {
			wrapped.addBatch();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void clearParameters() throws SQLException {
		try {
			wrapped.clearParameters();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public boolean execute() throws SQLException {
		try {
			return new Execute(this).execute();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public ResultSet executeQuery() throws SQLException {
		try {
				return new UcanaccessResultSet(wrapped.executeQuery(), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int executeUpdate() throws SQLException {
		try {
			int y = new ExecuteUpdate(this).execute();
			return y;
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public Map<Integer, Blob> getBlobMap() {
		return blobMap;
	}
	
	public ResultSetMetaData getMetaData() throws SQLException {
		try {
			return wrapped.getMetaData();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public ParameterMetaData getParameterMetaData() throws SQLException {
		try {
			return wrapped.getParameterMetaData();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setArray(int arg0, Array arg1) throws SQLException {
		try {
			wrapped.setArray(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
		try {
			wrapped.setAsciiStream(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.setAsciiStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.setAsciiStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		try {
			wrapped.setBigDecimal(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		try {
			wrapped.setBinaryStream(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.setBinaryStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.setBinaryStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBlob(int idx, Blob value) throws SQLException {
		try {
			this.blobMap.put(idx - 1, value);
			wrapped.setBlob(idx, value);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBlob(int idx, InputStream is) throws SQLException {
		try {
			wrapped.setBlob(idx, is);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		try {
			wrapped.setBlob(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBoolean(int arg0, boolean arg1) throws SQLException {
		try {
			wrapped.setBoolean(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setByte(int arg0, byte arg1) throws SQLException {
		try {
			wrapped.setByte(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setBytes(int arg0, byte[] arg1) throws SQLException {
		try {
			wrapped.setBytes(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
		try {
			wrapped.setCharacterStream(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		try {
			wrapped.setCharacterStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.setCharacterStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setClob(int arg0, Clob arg1) throws SQLException {
		try {
			wrapped.setClob(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setClob(int arg0, Reader arg1) throws SQLException {
		try {
			wrapped.setClob(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
		try {
			wrapped.setClob(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setCursorName(String name) throws SQLException {
		try {
			wrapped.setCursorName(name);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setDate(int arg0, Date arg1) throws SQLException {
		try {
			wrapped.setDate(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
		try {
			wrapped.setDate(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setDouble(int arg0, double arg1) throws SQLException {
		try {
			wrapped.setDouble(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setEscapeProcessing(boolean enable) throws SQLException {
		try {
			wrapped.setEscapeProcessing(enable);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setFetchDirection(int direction) throws SQLException {
		try {
			wrapped.setFetchDirection(direction);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setFetchSize(int rows) throws SQLException {
		try {
			wrapped.setFetchSize(rows);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setFloat(int arg0, float arg1) throws SQLException {
		try {
			wrapped.setFloat(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setInt(int arg0, int arg1) throws SQLException {
		try {
			wrapped.setInt(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setLong(int arg0, long arg1) throws SQLException {
		try {
			wrapped.setLong(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setMaxFieldSize(int max) throws SQLException {
		try {
			wrapped.setMaxFieldSize(max);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setMaxRows(int max) throws SQLException {
		try {
			wrapped.setMaxRows(max);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
		try {
			wrapped.setNCharacterStream(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		try {
			wrapped.setNCharacterStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNClob(int arg0, NClob arg1) throws SQLException {
		try {
			wrapped.setNClob(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNClob(int arg0, Reader arg1) throws SQLException {
		try {
			wrapped.setNClob(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		try {
			wrapped.setNClob(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNString(int arg0, String arg1) throws SQLException {
		try {
			wrapped.setNString(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNull(int arg0, int arg1) throws SQLException {
		try {
			wrapped.setNull(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setNull(int arg0, int arg1, String arg2) throws SQLException {
		try {
			wrapped.setNull(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setObject(int arg0, Object arg1) throws SQLException {
		try {
			wrapped.setObject(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
		try {
			wrapped.setObject(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setObject(int arg0, Object arg1, int arg2, int arg3)
			throws SQLException {
		try {
			wrapped.setObject(arg0, arg1, arg2, arg3);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setRef(int arg0, Ref arg1) throws SQLException {
		try {
			wrapped.setRef(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setRowId(int arg0, RowId arg1) throws SQLException {
		try {
			wrapped.setRowId(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setShort(int arg0, short arg1) throws SQLException {
		try {
			wrapped.setShort(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		try {
			wrapped.setSQLXML(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setString(int arg0, String arg1) throws SQLException {
		try {
			wrapped.setString(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setTime(int arg0, Time arg1) throws SQLException {
		try {
			wrapped.setTime(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
		try {
			wrapped.setTime(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
		try {
			wrapped.setTimestamp(arg0, arg1);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setTimestamp(int idx, Timestamp arg1, Calendar arg2)
			throws SQLException {
		try {
			wrapped.setTimestamp(idx, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	@Deprecated
	public void setUnicodeStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		try {
			wrapped.setUnicodeStream(arg0, arg1, arg2);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void setURL(int idx, URL url) throws SQLException {
		try {
			wrapped.setString(idx, "#" + url.toString() + "#");
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
}
