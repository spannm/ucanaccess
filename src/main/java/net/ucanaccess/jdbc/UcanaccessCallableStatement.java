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


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;



public class UcanaccessCallableStatement extends UcanaccessPreparedStatement implements
		CallableStatement {
	

	private CallableStatement hidden;
	
	
	public UcanaccessCallableStatement(NormalizedSQL nsql,
			CallableStatement hidden, UcanaccessConnection connection)
			throws SQLException {
		super(nsql, hidden, connection);
		this.hidden=hidden;
	}

		

	public void setShort(String parameterName, short x) throws SQLException {
		hidden.setShort(parameterName, x);
	}



	public boolean wasNull() throws SQLException {
		return hidden.wasNull();
	}



	
	
	public Array getArray(int parameterIndex) throws SQLException {
		return hidden.getArray(parameterIndex);
	}

	public Array getArray(String parameterName) throws SQLException {
		return hidden.getArray(parameterName);
	}
   @SuppressWarnings("deprecation")
	public BigDecimal getBigDecimal(int parameterIndex, int scale)
			throws SQLException {
		return hidden.getBigDecimal(parameterIndex, scale);
	}

	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return hidden.getBigDecimal(parameterIndex);
	}

	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return hidden.getBigDecimal(parameterName);
	}

	public Blob getBlob(int parameterIndex) throws SQLException {
		return hidden.getBlob(parameterIndex);
	}

	public Blob getBlob(String parameterName) throws SQLException {
		return hidden.getBlob(parameterName);
	}

	public boolean getBoolean(int parameterIndex) throws SQLException {
		return hidden.getBoolean(parameterIndex);
	}

	public boolean getBoolean(String parameterName) throws SQLException {
		return hidden.getBoolean(parameterName);
	}

	public byte getByte(int parameterIndex) throws SQLException {
		return hidden.getByte(parameterIndex);
	}

	public byte getByte(String parameterName) throws SQLException {
		return hidden.getByte(parameterName);
	}

	public byte[] getBytes(int parameterIndex) throws SQLException {
		return hidden.getBytes(parameterIndex);
	}

	public byte[] getBytes(String parameterName) throws SQLException {
		return hidden.getBytes(parameterName);
	}

	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return hidden.getCharacterStream(parameterIndex);
	}

	public Reader getCharacterStream(String parameterName) throws SQLException {
		return hidden.getCharacterStream(parameterName);
	}

	public Clob getClob(int parameterIndex) throws SQLException {
		return hidden.getClob(parameterIndex);
	}

	public Clob getClob(String parameterName) throws SQLException {
		return hidden.getClob(parameterName);
	}

	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return hidden.getDate(parameterIndex, cal);
	}

	public Date getDate(int parameterIndex) throws SQLException {
		return hidden.getDate(parameterIndex);
	}

	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return hidden.getDate(parameterName, cal);
	}

	public Date getDate(String parameterName) throws SQLException {
		return hidden.getDate(parameterName);
	}

	public double getDouble(int parameterIndex) throws SQLException {
		return hidden.getDouble(parameterIndex);
	}

	public double getDouble(String parameterName) throws SQLException {
		return hidden.getDouble(parameterName);
	}

	
	public float getFloat(int parameterIndex) throws SQLException {
		return hidden.getFloat(parameterIndex);
	}

	public float getFloat(String parameterName) throws SQLException {
		return hidden.getFloat(parameterName);
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		return hidden.getGeneratedKeys();
	}

	public int getInt(int parameterIndex) throws SQLException {
		return hidden.getInt(parameterIndex);
	}

	public int getInt(String parameterName) throws SQLException {
		return hidden.getInt(parameterName);
	}

	public long getLong(int parameterIndex) throws SQLException {
		return hidden.getLong(parameterIndex);
	}

	public long getLong(String parameterName) throws SQLException {
		return hidden.getLong(parameterName);
	}

	public int getMaxFieldSize() throws SQLException {
		return hidden.getMaxFieldSize();
	}

	public int getMaxRows() throws SQLException {
		return hidden.getMaxRows();
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return hidden.getMetaData();
	}

	public boolean getMoreResults() throws SQLException {
		return hidden.getMoreResults();
	}

	public boolean getMoreResults(int arg0) throws SQLException {
		return hidden.getMoreResults(arg0);
	}

	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return hidden.getNCharacterStream(parameterIndex);
	}

	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return hidden.getNCharacterStream(parameterName);
	}

	public NClob getNClob(int parameterIndex) throws SQLException {
		return hidden.getNClob(parameterIndex);
	}

	public NClob getNClob(String parameterName) throws SQLException {
		return hidden.getNClob(parameterName);
	}

	public String getNString(int parameterIndex) throws SQLException {
		return hidden.getNString(parameterIndex);
	}

	public String getNString(String parameterName) throws SQLException {
		return hidden.getNString(parameterName);
	}

	public Object getObject(int parameterIndex, Map<String, Class<?>> map)
			throws SQLException {
		return hidden.getObject(parameterIndex, map);
	}

	public Object getObject(int parameterIndex) throws SQLException {
		return hidden.getObject(parameterIndex);
	}

	public Object getObject(String parameterName, Map<String, Class<?>> map)
			throws SQLException {
		return hidden.getObject(parameterName, map);
	}

	public Object getObject(String parameterName) throws SQLException {
		return hidden.getObject(parameterName);
	}



	public Ref getRef(int parameterIndex) throws SQLException {
		return hidden.getRef(parameterIndex);
	}

	public Ref getRef(String parameterName) throws SQLException {
		return hidden.getRef(parameterName);
	}

	public ResultSet getResultSet() throws SQLException {
		return hidden.getResultSet();
	}

	

	public RowId getRowId(int parameterIndex) throws SQLException {
		return hidden.getRowId(parameterIndex);
	}

	public RowId getRowId(String parameterName) throws SQLException {
		return hidden.getRowId(parameterName);
	}

	public short getShort(int parameterIndex) throws SQLException {
		return hidden.getShort(parameterIndex);
	}

	public short getShort(String parameterName) throws SQLException {
		return hidden.getShort(parameterName);
	}

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return hidden.getSQLXML(parameterIndex);
	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return hidden.getSQLXML(parameterName);
	}

	public String getString(int parameterIndex) throws SQLException {
		return hidden.getString(parameterIndex);
	}

	public String getString(String parameterName) throws SQLException {
		return hidden.getString(parameterName);
	}

	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return hidden.getTime(parameterIndex, cal);
	}

	public Time getTime(int parameterIndex) throws SQLException {
		return hidden.getTime(parameterIndex);
	}

	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return hidden.getTime(parameterName, cal);
	}

	public Time getTime(String parameterName) throws SQLException {
		return hidden.getTime(parameterName);
	}

	public Timestamp getTimestamp(int parameterIndex, Calendar cal)
			throws SQLException {
		return hidden.getTimestamp(parameterIndex, cal);
	}

	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return hidden.getTimestamp(parameterIndex);
	}

	public Timestamp getTimestamp(String parameterName, Calendar cal)
			throws SQLException {
		return hidden.getTimestamp(parameterName, cal);
	}

	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return hidden.getTimestamp(parameterName);
	}

	

	public URL getURL(int parameterIndex) throws SQLException {
		return hidden.getURL(parameterIndex);
	}

	public URL getURL(String parameterName) throws SQLException {
		return hidden.getURL(parameterName);
	}

	

	public void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		hidden.registerOutParameter(parameterIndex, sqlType, scale);
	}

	public void registerOutParameter(int parameterIndex, int sqlType,
			String typeName) throws SQLException {
		hidden.registerOutParameter(parameterIndex, sqlType, typeName);
	}

	public void registerOutParameter(int parameterIndex, int sqlType)
			throws SQLException {
		hidden.registerOutParameter(parameterIndex, sqlType);
	}

	public void registerOutParameter(String parameterName, int sqlType,
			int scale) throws SQLException {
		hidden.registerOutParameter(parameterName, sqlType, scale);
	}

	public void registerOutParameter(String parameterName, int sqlType,
			String typeName) throws SQLException {
		hidden.registerOutParameter(parameterName, sqlType, typeName);
	}

	public void registerOutParameter(String parameterName, int sqlType)
			throws SQLException {
		hidden.registerOutParameter(parameterName, sqlType);
	}

	
	
	public void setAsciiStream(String parameterName, InputStream x, int length)
			throws SQLException {
		hidden.setAsciiStream(parameterName, x, length);
	}

	public void setAsciiStream(String parameterName, InputStream x, long length)
			throws SQLException {
		hidden.setAsciiStream(parameterName, x, length);
	}

	public void setAsciiStream(String parameterName, InputStream x)
			throws SQLException {
		hidden.setAsciiStream(parameterName, x);
	}

	

	public void setBigDecimal(String parameterName, BigDecimal x)
			throws SQLException {
		hidden.setBigDecimal(parameterName, x);
	}

	

	public void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		hidden.setBinaryStream(parameterName, x, length);
	}

	public void setBinaryStream(String parameterName, InputStream x, long length)
			throws SQLException {
		hidden.setBinaryStream(parameterName, x, length);
	}

	public void setBinaryStream(String parameterName, InputStream x)
			throws SQLException {
		hidden.setBinaryStream(parameterName, x);
	}

	

	public void setBlob(String parameterName, Blob x) throws SQLException {
		hidden.setBlob(parameterName, x);
	}

	public void setBlob(String parameterName, InputStream inputStream,
			long length) throws SQLException {
		hidden.setBlob(parameterName, inputStream, length);
	}

	public void setBlob(String parameterName, InputStream inputStream)
			throws SQLException {
		hidden.setBlob(parameterName, inputStream);
	}

	
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		hidden.setBoolean(parameterName, x);
	}

	

	public void setByte(String parameterName, byte x) throws SQLException {
		hidden.setByte(parameterName, x);
	}

	
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		hidden.setBytes(parameterName, x);
	}


	public void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		hidden.setCharacterStream(parameterName, reader, length);
	}

	public void setCharacterStream(String parameterName, Reader reader,
			long length) throws SQLException {
		hidden.setCharacterStream(parameterName, reader, length);
	}

	public void setCharacterStream(String parameterName, Reader reader)
			throws SQLException {
		hidden.setCharacterStream(parameterName, reader);
	}

	
	public void setClob(String parameterName, Clob x) throws SQLException {
		hidden.setClob(parameterName, x);
	}

	public void setClob(String parameterName, Reader reader, long length)
			throws SQLException {
		hidden.setClob(parameterName, reader, length);
	}

	public void setClob(String parameterName, Reader reader)
			throws SQLException {
		hidden.setClob(parameterName, reader);
	}

	
	public void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		hidden.setDate(parameterName, x, cal);
	}

	public void setDate(String parameterName, Date x) throws SQLException {
		hidden.setDate(parameterName, x);
	}

	
	public void setDouble(String parameterName, double x) throws SQLException {
		hidden.setDouble(parameterName, x);
	}

	

	public void setFloat(String parameterName, float x) throws SQLException {
		hidden.setFloat(parameterName, x);
	}

	public void setInt(int arg0, int arg1) throws SQLException {
		hidden.setInt(arg0, arg1);
	}

	public void setInt(String parameterName, int x) throws SQLException {
		hidden.setInt(parameterName, x);
	}

	public void setLong(String parameterName, long x) throws SQLException {
		hidden.setLong(parameterName, x);
	}

	
	public void setNCharacterStream(String parameterName, Reader value,
			long length) throws SQLException {
		hidden.setNCharacterStream(parameterName, value, length);
	}

	public void setNCharacterStream(String parameterName, Reader value)
			throws SQLException {
		hidden.setNCharacterStream(parameterName, value);
	}



	public void setNClob(String parameterName, NClob value) throws SQLException {
		hidden.setNClob(parameterName, value);
	}

	public void setNClob(String parameterName, Reader reader, long length)
			throws SQLException {
		hidden.setNClob(parameterName, reader, length);
	}

	public void setNClob(String parameterName, Reader reader)
			throws SQLException {
		hidden.setNClob(parameterName, reader);
	}

	public void setNString(String parameterName, String value)
			throws SQLException {
		hidden.setNString(parameterName, value);
	}

	

	public void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		hidden.setNull(parameterName, sqlType, typeName);
	}

	public void setNull(String parameterName, int sqlType) throws SQLException {
		hidden.setNull(parameterName, sqlType);
	}

	

	public void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		hidden.setObject(parameterName, x, targetSqlType, scale);
	}

	public void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		hidden.setObject(parameterName, x, targetSqlType);
	}

	public void setObject(String parameterName, Object x) throws SQLException {
		hidden.setObject(parameterName, x);
	}

	

	

	public void setRowId(String parameterName, RowId x) throws SQLException {
		hidden.setRowId(parameterName, x);
	}

	

	public void setSQLXML(String parameterName, SQLXML xmlObject)
			throws SQLException {
		hidden.setSQLXML(parameterName, xmlObject);
	}

	

	public void setString(String parameterName, String x) throws SQLException {
		hidden.setString(parameterName, x);
	}

	

	public void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		hidden.setTime(parameterName, x, cal);
	}

	public void setTime(String parameterName, Time x) throws SQLException {
		hidden.setTime(parameterName, x);
	}

	

	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		hidden.setTimestamp(parameterName, x, cal);
	}

	public void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		hidden.setTimestamp(parameterName, x);
	}

	

	public void setURL(String parameterName, URL val) throws SQLException {
		hidden.setURL(parameterName, val);
	}



	@Override
	public boolean execute() throws SQLException {
		if(!UcanaccessConnection.hasContext()){
			UcanaccessConnection.setCtxConnection((UcanaccessConnection)super.getConnection());
		}
		return super.execute();
	}



	@Override
	public int executeUpdate() throws SQLException {
		if(!UcanaccessConnection.hasContext()){
			UcanaccessConnection.setCtxConnection((UcanaccessConnection)super.getConnection());
		}
		return super.executeUpdate();
	}
	



	/**
	 *<p>Returns an object representing the value of OUT parameter
	 * {@code parameterIndex} and will convert from the SQL type of the parameter
	 * to the requested Java data type, if the conversion is supported.
	 *<p>
	 * Added without Override annotation for compatibility with Java >= 7 compilers.
	 * @since 1.7
	 */
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException("Method not implemented. Requires Java >= 7.");
	}


	/**
	 *<p>Returns an object representing the value of OUT parameter
	 * {@code parameterName} and will convert from the SQL type of the parameter
	 * to the requested Java data type, if the conversion is supported.
	 *<p>
	 * Added without Override annotation for compatibility with Java >= 7 compilers.
	 * @since 1.7
	 */
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException("Method not implemented. Requires Java >= 7.");
	}

}
