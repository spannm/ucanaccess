package net.ucanaccess.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class UcanaccessCallableStatement extends UcanaccessPreparedStatement implements CallableStatement {

    private CallableStatement hidden;

    public UcanaccessCallableStatement(NormalizedSQL _nsql, CallableStatement _hidden, UcanaccessConnection _connection)
            throws SQLException {
        super(_nsql, _hidden, _connection);
        hidden = _hidden;
    }

    @Override
    public void setShort(String _parmName, short x) throws SQLException {
        hidden.setShort(_parmName, x);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return hidden.wasNull();
    }

    @Override
    public Array getArray(int _parmIdx) throws SQLException {
        return hidden.getArray(_parmIdx);
    }

    @Override
    public Array getArray(String _parmName) throws SQLException {
        return hidden.getArray(_parmName);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int _parmIdx, int scale) throws SQLException {
        return hidden.getBigDecimal(_parmIdx, scale);
    }

    @Override
    public BigDecimal getBigDecimal(int _parmIdx) throws SQLException {
        return hidden.getBigDecimal(_parmIdx);
    }

    @Override
    public BigDecimal getBigDecimal(String _parmName) throws SQLException {
        return hidden.getBigDecimal(_parmName);
    }

    @Override
    public Blob getBlob(int _parmIdx) throws SQLException {
        return hidden.getBlob(_parmIdx);
    }

    @Override
    public Blob getBlob(String _parmName) throws SQLException {
        return hidden.getBlob(_parmName);
    }

    @Override
    public boolean getBoolean(int _parmIdx) throws SQLException {
        return hidden.getBoolean(_parmIdx);
    }

    @Override
    public boolean getBoolean(String _parmName) throws SQLException {
        return hidden.getBoolean(_parmName);
    }

    @Override
    public byte getByte(int _parmIdx) throws SQLException {
        return hidden.getByte(_parmIdx);
    }

    @Override
    public byte getByte(String _parmName) throws SQLException {
        return hidden.getByte(_parmName);
    }

    @Override
    public byte[] getBytes(int _parmIdx) throws SQLException {
        return hidden.getBytes(_parmIdx);
    }

    @Override
    public byte[] getBytes(String _parmName) throws SQLException {
        return hidden.getBytes(_parmName);
    }

    @Override
    public Reader getCharacterStream(int _parmIdx) throws SQLException {
        return hidden.getCharacterStream(_parmIdx);
    }

    @Override
    public Reader getCharacterStream(String _parmName) throws SQLException {
        return hidden.getCharacterStream(_parmName);
    }

    @Override
    public Clob getClob(int _parmIdx) throws SQLException {
        return hidden.getClob(_parmIdx);
    }

    @Override
    public Clob getClob(String _parmName) throws SQLException {
        return hidden.getClob(_parmName);
    }

    @Override
    public Date getDate(int _parmIdx, Calendar cal) throws SQLException {
        return hidden.getDate(_parmIdx, cal);
    }

    @Override
    public Date getDate(int _parmIdx) throws SQLException {
        return hidden.getDate(_parmIdx);
    }

    @Override
    public Date getDate(String _parmName, Calendar cal) throws SQLException {
        return hidden.getDate(_parmName, cal);
    }

    @Override
    public Date getDate(String _parmName) throws SQLException {
        return hidden.getDate(_parmName);
    }

    @Override
    public double getDouble(int _parmIdx) throws SQLException {
        return hidden.getDouble(_parmIdx);
    }

    @Override
    public double getDouble(String _parmName) throws SQLException {
        return hidden.getDouble(_parmName);
    }

    @Override
    public float getFloat(int _parmIdx) throws SQLException {
        return hidden.getFloat(_parmIdx);
    }

    @Override
    public float getFloat(String _parmName) throws SQLException {
        return hidden.getFloat(_parmName);
    }

    @Override
    public ResultSet getGeneratedKeys() throws UcanaccessSQLException {
        return tryCatch(hidden::getGeneratedKeys);
    }

    @Override
    public int getInt(int _parmIdx) throws SQLException {
        return hidden.getInt(_parmIdx);
    }

    @Override
    public int getInt(String _parmName) throws SQLException {
        return hidden.getInt(_parmName);
    }

    @Override
    public long getLong(int _parmIdx) throws SQLException {
        return hidden.getLong(_parmIdx);
    }

    @Override
    public long getLong(String _parmName) throws SQLException {
        return hidden.getLong(_parmName);
    }

    @Override
    public int getMaxFieldSize() throws UcanaccessSQLException {
        return tryCatch(hidden::getMaxFieldSize);
    }

    @Override
    public int getMaxRows() throws UcanaccessSQLException {
        return tryCatch(hidden::getMaxRows);
    }

    @Override
    public ResultSetMetaData getMetaData() throws UcanaccessSQLException {
        return tryCatch(hidden::getMetaData);
    }

    @Override
    public boolean getMoreResults() throws UcanaccessSQLException {
        return tryCatch(() -> hidden.getMoreResults());
    }

    @Override
    public boolean getMoreResults(int _current) throws UcanaccessSQLException {
        return tryCatch(() -> hidden.getMoreResults(_current));
    }

    @Override
    public Reader getNCharacterStream(int _parmIdx) throws SQLException {
        return hidden.getNCharacterStream(_parmIdx);
    }

    @Override
    public Reader getNCharacterStream(String _parmName) throws SQLException {
        return hidden.getNCharacterStream(_parmName);
    }

    @Override
    public NClob getNClob(int _parmIdx) throws SQLException {
        return hidden.getNClob(_parmIdx);
    }

    @Override
    public NClob getNClob(String _parmName) throws SQLException {
        return hidden.getNClob(_parmName);
    }

    @Override
    public String getNString(int _parmIdx) throws SQLException {
        return hidden.getNString(_parmIdx);
    }

    @Override
    public String getNString(String _parmName) throws SQLException {
        return hidden.getNString(_parmName);
    }

    @Override
    public Object getObject(int _parmIdx, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(_parmIdx, map);
    }

    @Override
    public Object getObject(int _parmIdx) throws SQLException {
        return hidden.getObject(_parmIdx);
    }

    @Override
    public Object getObject(String _parmName, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(_parmName, map);
    }

    @Override
    public Object getObject(String _parmName) throws SQLException {
        return hidden.getObject(_parmName);
    }

    @Override
    public <T> T getObject(int _parmIdx, Class<T> type) throws SQLException {
        return hidden.getObject(_parmIdx, type);
    }

    @Override
    public <T> T getObject(String _parmName, Class<T> type) throws SQLException {
        return hidden.getObject(_parmName, type);
    }

    @Override
    public Ref getRef(int _parmIdx) throws SQLException {
        return hidden.getRef(_parmIdx);
    }

    @Override
    public Ref getRef(String _parmName) throws SQLException {
        return hidden.getRef(_parmName);
    }

    @Override
    public ResultSet getResultSet() throws UcanaccessSQLException {
        return tryCatch(hidden::getResultSet);
    }

    @Override
    public RowId getRowId(int _parmIdx) throws SQLException {
        return hidden.getRowId(_parmIdx);
    }

    @Override
    public RowId getRowId(String _parmName) throws SQLException {
        return hidden.getRowId(_parmName);
    }

    @Override
    public short getShort(int _parmIdx) throws SQLException {
        return hidden.getShort(_parmIdx);
    }

    @Override
    public short getShort(String _parmName) throws SQLException {
        return hidden.getShort(_parmName);
    }

    @Override
    public SQLXML getSQLXML(int _parmIdx) throws SQLException {
        return hidden.getSQLXML(_parmIdx);
    }

    @Override
    public SQLXML getSQLXML(String _parmName) throws SQLException {
        return hidden.getSQLXML(_parmName);
    }

    @Override
    public String getString(int _parmIdx) throws SQLException {
        return hidden.getString(_parmIdx);
    }

    @Override
    public String getString(String _parmName) throws SQLException {
        return hidden.getString(_parmName);
    }

    @Override
    public Time getTime(int _parmIdx, Calendar cal) throws SQLException {
        return hidden.getTime(_parmIdx, cal);
    }

    @Override
    public Time getTime(int _parmIdx) throws SQLException {
        return hidden.getTime(_parmIdx);
    }

    @Override
    public Time getTime(String _parmName, Calendar cal) throws SQLException {
        return hidden.getTime(_parmName, cal);
    }

    @Override
    public Time getTime(String _parmName) throws SQLException {
        return hidden.getTime(_parmName);
    }

    @Override
    public Timestamp getTimestamp(int _parmIdx, Calendar cal) throws SQLException {
        return hidden.getTimestamp(_parmIdx, cal);
    }

    @Override
    public Timestamp getTimestamp(int _parmIdx) throws SQLException {
        return hidden.getTimestamp(_parmIdx);
    }

    @Override
    public Timestamp getTimestamp(String _parmName, Calendar cal) throws SQLException {
        return hidden.getTimestamp(_parmName, cal);
    }

    @Override
    public Timestamp getTimestamp(String _parmName) throws SQLException {
        return hidden.getTimestamp(_parmName);
    }

    @Override
    public URL getURL(int _parmIdx) throws SQLException {
        return hidden.getURL(_parmIdx);
    }

    @Override
    public URL getURL(String _parmName) throws SQLException {
        return hidden.getURL(_parmName);
    }

    @Override
    public void registerOutParameter(int _parmIdx, int _sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(_parmIdx, _sqlType, scale);
    }

    @Override
    public void registerOutParameter(int _parmIdx, int _sqlType, String _typeName) throws SQLException {
        hidden.registerOutParameter(_parmIdx, _sqlType, _typeName);
    }

    @Override
    public void registerOutParameter(int _parmIdx, int _sqlType) throws SQLException {
        hidden.registerOutParameter(_parmIdx, _sqlType);
    }

    @Override
    public void registerOutParameter(String _parmName, int _sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(_parmName, _sqlType, scale);
    }

    @Override
    public void registerOutParameter(String _parmName, int _sqlType, String _typeName) throws SQLException {
        hidden.registerOutParameter(_parmName, _sqlType, _typeName);
    }

    @Override
    public void registerOutParameter(String _parmName, int _sqlType) throws SQLException {
        hidden.registerOutParameter(_parmName, _sqlType);
    }

    @Override
    public void setAsciiStream(String _parmName, InputStream x, int length) throws SQLException {
        hidden.setAsciiStream(_parmName, x, length);
    }

    @Override
    public void setAsciiStream(String _parmName, InputStream x, long length) throws SQLException {
        hidden.setAsciiStream(_parmName, x, length);
    }

    @Override
    public void setAsciiStream(String _parmName, InputStream x) throws SQLException {
        hidden.setAsciiStream(_parmName, x);
    }

    @Override
    public void setBigDecimal(String _parmName, BigDecimal x) throws SQLException {
        hidden.setBigDecimal(_parmName, x);
    }

    @Override
    public void setBinaryStream(String _parmName, InputStream x, int length) throws SQLException {
        hidden.setBinaryStream(_parmName, x, length);
    }

    @Override
    public void setBinaryStream(String _parmName, InputStream x, long length) throws SQLException {
        hidden.setBinaryStream(_parmName, x, length);
    }

    @Override
    public void setBinaryStream(String _parmName, InputStream x) throws SQLException {
        hidden.setBinaryStream(_parmName, x);
    }

    @Override
    public void setBlob(String _parmName, Blob x) throws SQLException {
        hidden.setBlob(_parmName, x);
    }

    @Override
    public void setBlob(String _parmName, InputStream inputStream, long length) throws SQLException {
        hidden.setBlob(_parmName, inputStream, length);
    }

    @Override
    public void setBlob(String _parmName, InputStream inputStream) throws SQLException {
        hidden.setBlob(_parmName, inputStream);
    }

    @Override
    public void setBoolean(String _parmName, boolean x) throws SQLException {
        hidden.setBoolean(_parmName, x);
    }

    @Override
    public void setByte(String _parmName, byte x) throws SQLException {
        hidden.setByte(_parmName, x);
    }

    @Override
    public void setBytes(String _parmName, byte[] x) throws SQLException {
        hidden.setBytes(_parmName, x);
    }

    @Override
    public void setCharacterStream(String _parmName, Reader reader, int length) throws SQLException {
        hidden.setCharacterStream(_parmName, reader, length);
    }

    @Override
    public void setCharacterStream(String _parmName, Reader reader, long length) throws SQLException {
        hidden.setCharacterStream(_parmName, reader, length);
    }

    @Override
    public void setCharacterStream(String _parmName, Reader reader) throws SQLException {
        hidden.setCharacterStream(_parmName, reader);
    }

    @Override
    public void setClob(String _parmName, Clob x) throws SQLException {
        hidden.setClob(_parmName, x);
    }

    @Override
    public void setClob(String _parmName, Reader reader, long length) throws SQLException {
        hidden.setClob(_parmName, reader, length);
    }

    @Override
    public void setClob(String _parmName, Reader reader) throws SQLException {
        hidden.setClob(_parmName, reader);
    }

    @Override
    public void setDate(String _parmName, Date x, Calendar cal) throws SQLException {
        hidden.setDate(_parmName, x, cal);
    }

    @Override
    public void setDate(String _parmName, Date x) throws SQLException {
        hidden.setDate(_parmName, x);
    }

    @Override
    public void setDouble(String _parmName, double x) throws SQLException {
        hidden.setDouble(_parmName, x);
    }

    @Override
    public void setFloat(String _parmName, float x) throws SQLException {
        hidden.setFloat(_parmName, x);
    }

    @Override
    public void setInt(int _parmIdx, int arg1) throws UcanaccessSQLException {
        tryCatch(() -> hidden.setInt(_parmIdx, arg1));
    }

    @Override
    public void setInt(String _parmName, int x) throws SQLException {
        hidden.setInt(_parmName, x);
    }

    @Override
    public void setLong(String _parmName, long x) throws SQLException {
        hidden.setLong(_parmName, x);
    }

    @Override
    public void setNCharacterStream(String _parmName, Reader value, long length) throws SQLException {
        hidden.setNCharacterStream(_parmName, value, length);
    }

    @Override
    public void setNCharacterStream(String _parmName, Reader value) throws SQLException {
        hidden.setNCharacterStream(_parmName, value);
    }

    @Override
    public void setNClob(String _parmName, NClob value) throws SQLException {
        hidden.setNClob(_parmName, value);
    }

    @Override
    public void setNClob(String _parmName, Reader reader, long length) throws SQLException {
        hidden.setNClob(_parmName, reader, length);
    }

    @Override
    public void setNClob(String _parmName, Reader reader) throws SQLException {
        hidden.setNClob(_parmName, reader);
    }

    @Override
    public void setNString(String _parmName, String value) throws SQLException {
        hidden.setNString(_parmName, value);
    }

    @Override
    public void setNull(String _parmName, int _sqlType, String _typeName) throws SQLException {
        hidden.setNull(_parmName, _sqlType, _typeName);
    }

    @Override
    public void setNull(String _parmName, int _sqlType) throws SQLException {
        hidden.setNull(_parmName, _sqlType);
    }

    @Override
    public void setObject(String _parmName, Object x, int targetSqlType, int scale) throws SQLException {
        hidden.setObject(_parmName, x, targetSqlType, scale);
    }

    @Override
    public void setObject(String _parmName, Object x, int targetSqlType) throws SQLException {
        hidden.setObject(_parmName, x, targetSqlType);
    }

    @Override
    public void setObject(String _parmName, Object x) throws SQLException {
        hidden.setObject(_parmName, x);
    }

    @Override
    public void setRowId(String _parmName, RowId x) throws SQLException {
        hidden.setRowId(_parmName, x);
    }

    @Override
    public void setSQLXML(String _parmName, SQLXML xmlObject) throws SQLException {
        hidden.setSQLXML(_parmName, xmlObject);
    }

    @Override
    public void setString(String _parmName, String x) throws SQLException {
        hidden.setString(_parmName, x);
    }

    @Override
    public void setTime(String _parmName, Time x, Calendar cal) throws SQLException {
        hidden.setTime(_parmName, x, cal);
    }

    @Override
    public void setTime(String _parmName, Time x) throws SQLException {
        hidden.setTime(_parmName, x);
    }

    @Override
    public void setTimestamp(String _parmName, Timestamp x, Calendar cal) throws SQLException {
        hidden.setTimestamp(_parmName, x, cal);
    }

    @Override
    public void setTimestamp(String _parmName, Timestamp x) throws SQLException {
        hidden.setTimestamp(_parmName, x);
    }

    @Override
    public void setURL(String _parmName, URL val) throws SQLException {
        hidden.setURL(_parmName, val);
    }

    @Override
    public boolean execute() throws UcanaccessSQLException {
        if (!UcanaccessConnection.hasContext()) {
            UcanaccessConnection.setCtxConnection(super.getConnection());
        }
        return super.execute();
    }

    @Override
    public int executeUpdate() throws UcanaccessSQLException {
        if (!UcanaccessConnection.hasContext()) {
            UcanaccessConnection.setCtxConnection(super.getConnection());
        }
        return super.executeUpdate();
    }

}
