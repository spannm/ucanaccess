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

public class UcanaccessCallableStatement extends UcanaccessPreparedStatement implements CallableStatement {

    private CallableStatement hidden;

    public UcanaccessCallableStatement(NormalizedSQL _nsql, CallableStatement _hidden, UcanaccessConnection _connection)
            throws SQLException {
        super(_nsql, _hidden, _connection);
        this.hidden = _hidden;
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        hidden.setShort(parameterName, x);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return hidden.wasNull();
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return hidden.getArray(parameterIndex);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return hidden.getArray(parameterName);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return hidden.getBigDecimal(parameterIndex, scale);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return hidden.getBigDecimal(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return hidden.getBigDecimal(parameterName);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return hidden.getBlob(parameterIndex);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return hidden.getBlob(parameterName);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return hidden.getBoolean(parameterIndex);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return hidden.getBoolean(parameterName);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return hidden.getByte(parameterIndex);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return hidden.getByte(parameterName);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return hidden.getBytes(parameterIndex);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return hidden.getBytes(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return hidden.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return hidden.getCharacterStream(parameterName);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return hidden.getClob(parameterIndex);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return hidden.getClob(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return hidden.getDate(parameterIndex, cal);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return hidden.getDate(parameterIndex);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return hidden.getDate(parameterName, cal);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return hidden.getDate(parameterName);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return hidden.getDouble(parameterIndex);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return hidden.getDouble(parameterName);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return hidden.getFloat(parameterIndex);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return hidden.getFloat(parameterName);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return hidden.getGeneratedKeys();
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return hidden.getInt(parameterIndex);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return hidden.getInt(parameterName);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return hidden.getLong(parameterIndex);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return hidden.getLong(parameterName);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return hidden.getMaxFieldSize();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return hidden.getMaxRows();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return hidden.getMetaData();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return hidden.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        return hidden.getMoreResults(arg0);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return hidden.getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return hidden.getNCharacterStream(parameterName);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return hidden.getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return hidden.getNClob(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return hidden.getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return hidden.getNString(parameterName);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(parameterIndex, map);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return hidden.getObject(parameterIndex);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(parameterName, map);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return hidden.getObject(parameterName);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return hidden.getRef(parameterIndex);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return hidden.getRef(parameterName);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return hidden.getResultSet();
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return hidden.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return hidden.getRowId(parameterName);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return hidden.getShort(parameterIndex);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return hidden.getShort(parameterName);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return hidden.getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return hidden.getSQLXML(parameterName);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return hidden.getString(parameterIndex);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return hidden.getString(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return hidden.getTime(parameterIndex, cal);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return hidden.getTime(parameterIndex);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return hidden.getTime(parameterName, cal);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return hidden.getTime(parameterName);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return hidden.getTimestamp(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return hidden.getTimestamp(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return hidden.getTimestamp(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return hidden.getTimestamp(parameterName);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return hidden.getURL(parameterIndex);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return hidden.getURL(parameterName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(parameterIndex, sqlType, scale);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        hidden.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        hidden.registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        hidden.registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        hidden.registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        hidden.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        hidden.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        hidden.setAsciiStream(parameterName, x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        hidden.setBigDecimal(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        hidden.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        hidden.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        hidden.setBinaryStream(parameterName, x);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        hidden.setBlob(parameterName, x);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        hidden.setBlob(parameterName, inputStream, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        hidden.setBlob(parameterName, inputStream);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        hidden.setBoolean(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        hidden.setByte(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        hidden.setBytes(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        hidden.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        hidden.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        hidden.setCharacterStream(parameterName, reader);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        hidden.setClob(parameterName, x);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        hidden.setClob(parameterName, reader, length);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        hidden.setClob(parameterName, reader);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        hidden.setDate(parameterName, x, cal);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        hidden.setDate(parameterName, x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        hidden.setDouble(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        hidden.setFloat(parameterName, x);
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {
        hidden.setInt(arg0, arg1);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        hidden.setInt(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        hidden.setLong(parameterName, x);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        hidden.setNCharacterStream(parameterName, value, length);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        hidden.setNCharacterStream(parameterName, value);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        hidden.setNClob(parameterName, value);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        hidden.setNClob(parameterName, reader, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        hidden.setNClob(parameterName, reader);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        hidden.setNString(parameterName, value);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        hidden.setNull(parameterName, sqlType, typeName);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        hidden.setNull(parameterName, sqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        hidden.setObject(parameterName, x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        hidden.setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        hidden.setObject(parameterName, x);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        hidden.setRowId(parameterName, x);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        hidden.setSQLXML(parameterName, xmlObject);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        hidden.setString(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        hidden.setTime(parameterName, x, cal);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        hidden.setTime(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        hidden.setTimestamp(parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        hidden.setTimestamp(parameterName, x);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        hidden.setURL(parameterName, val);
    }

    @Override
    public boolean execute() throws SQLException {
        if (!UcanaccessConnection.hasContext()) {
            UcanaccessConnection.setCtxConnection((UcanaccessConnection) super.getConnection());
        }
        return super.execute();
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!UcanaccessConnection.hasContext()) {
            UcanaccessConnection.setCtxConnection((UcanaccessConnection) super.getConnection());
        }
        return super.executeUpdate();
    }

    /**
     * <p>
     * Returns an object representing the value of OUT parameter {@code parameterIndex} and will convert from the SQL
     * type of the parameter to the requested Java data type, if the conversion is supported.
     * <p>
     * Added without Override annotation for compatibility with Java >= 7 compilers.
     *
     * @since 1.7
     */
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Method not implemented. Requires Java >= 7.");
    }

    /**
     * <p>
     * Returns an object representing the value of OUT parameter {@code parameterName} and will convert from the SQL
     * type of the parameter to the requested Java data type, if the conversion is supported.
     * <p>
     * Added without Override annotation for compatibility with Java >= 7 compilers.
     *
     * @since 1.7
     */
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Method not implemented. Requires Java >= 7.");
    }

}
