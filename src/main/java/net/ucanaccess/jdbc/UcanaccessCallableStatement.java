package net.ucanaccess.jdbc;

import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class UcanaccessCallableStatement extends UcanaccessPreparedStatement implements CallableStatement {

    private CallableStatement hidden;

    public UcanaccessCallableStatement(NormalizedSQL nsql, CallableStatement hidden, UcanaccessConnection connection)
            throws SQLException {
        super(nsql, hidden, connection);
        this.hidden = hidden;
    }

    @Override
    public void setShort(String parmName, short x) throws SQLException {
        hidden.setShort(parmName, x);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return hidden.wasNull();
    }

    @Override
    public Array getArray(int parmIdx) throws SQLException {
        return hidden.getArray(parmIdx);
    }

    @Override
    public Array getArray(String parmName) throws SQLException {
        return hidden.getArray(parmName);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parmIdx, int scale) throws SQLException {
        return hidden.getBigDecimal(parmIdx, scale);
    }

    @Override
    public BigDecimal getBigDecimal(int parmIdx) throws SQLException {
        return hidden.getBigDecimal(parmIdx);
    }

    @Override
    public BigDecimal getBigDecimal(String parmName) throws SQLException {
        return hidden.getBigDecimal(parmName);
    }

    @Override
    public Blob getBlob(int parmIdx) throws SQLException {
        return hidden.getBlob(parmIdx);
    }

    @Override
    public Blob getBlob(String parmName) throws SQLException {
        return hidden.getBlob(parmName);
    }

    @Override
    public boolean getBoolean(int parmIdx) throws SQLException {
        return hidden.getBoolean(parmIdx);
    }

    @Override
    public boolean getBoolean(String parmName) throws SQLException {
        return hidden.getBoolean(parmName);
    }

    @Override
    public byte getByte(int parmIdx) throws SQLException {
        return hidden.getByte(parmIdx);
    }

    @Override
    public byte getByte(String parmName) throws SQLException {
        return hidden.getByte(parmName);
    }

    @Override
    public byte[] getBytes(int parmIdx) throws SQLException {
        return hidden.getBytes(parmIdx);
    }

    @Override
    public byte[] getBytes(String parmName) throws SQLException {
        return hidden.getBytes(parmName);
    }

    @Override
    public Reader getCharacterStream(int parmIdx) throws SQLException {
        return hidden.getCharacterStream(parmIdx);
    }

    @Override
    public Reader getCharacterStream(String parmName) throws SQLException {
        return hidden.getCharacterStream(parmName);
    }

    @Override
    public Clob getClob(int parmIdx) throws SQLException {
        return hidden.getClob(parmIdx);
    }

    @Override
    public Clob getClob(String parmName) throws SQLException {
        return hidden.getClob(parmName);
    }

    @Override
    public Date getDate(int parmIdx, Calendar cal) throws SQLException {
        return hidden.getDate(parmIdx, cal);
    }

    @Override
    public Date getDate(int parmIdx) throws SQLException {
        return hidden.getDate(parmIdx);
    }

    @Override
    public Date getDate(String parmName, Calendar cal) throws SQLException {
        return hidden.getDate(parmName, cal);
    }

    @Override
    public Date getDate(String parmName) throws SQLException {
        return hidden.getDate(parmName);
    }

    @Override
    public double getDouble(int parmIdx) throws SQLException {
        return hidden.getDouble(parmIdx);
    }

    @Override
    public double getDouble(String parmName) throws SQLException {
        return hidden.getDouble(parmName);
    }

    @Override
    public float getFloat(int parmIdx) throws SQLException {
        return hidden.getFloat(parmIdx);
    }

    @Override
    public float getFloat(String parmName) throws SQLException {
        return hidden.getFloat(parmName);
    }

    @Override
    public ResultSet getGeneratedKeys() throws UcanaccessSQLException {
        return tryCatch(hidden::getGeneratedKeys);
    }

    @Override
    public int getInt(int parmIdx) throws SQLException {
        return hidden.getInt(parmIdx);
    }

    @Override
    public int getInt(String parmName) throws SQLException {
        return hidden.getInt(parmName);
    }

    @Override
    public long getLong(int parmIdx) throws SQLException {
        return hidden.getLong(parmIdx);
    }

    @Override
    public long getLong(String parmName) throws SQLException {
        return hidden.getLong(parmName);
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
    public boolean getMoreResults(int current) throws UcanaccessSQLException {
        return tryCatch(() -> hidden.getMoreResults(current));
    }

    @Override
    public Reader getNCharacterStream(int parmIdx) throws SQLException {
        return hidden.getNCharacterStream(parmIdx);
    }

    @Override
    public Reader getNCharacterStream(String parmName) throws SQLException {
        return hidden.getNCharacterStream(parmName);
    }

    @Override
    public NClob getNClob(int parmIdx) throws SQLException {
        return hidden.getNClob(parmIdx);
    }

    @Override
    public NClob getNClob(String parmName) throws SQLException {
        return hidden.getNClob(parmName);
    }

    @Override
    public String getNString(int parmIdx) throws SQLException {
        return hidden.getNString(parmIdx);
    }

    @Override
    public String getNString(String parmName) throws SQLException {
        return hidden.getNString(parmName);
    }

    @Override
    public Object getObject(int parmIdx, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(parmIdx, map);
    }

    @Override
    public Object getObject(int parmIdx) throws SQLException {
        return hidden.getObject(parmIdx);
    }

    @Override
    public Object getObject(String parmName, Map<String, Class<?>> map) throws SQLException {
        return hidden.getObject(parmName, map);
    }

    @Override
    public Object getObject(String parmName) throws SQLException {
        return hidden.getObject(parmName);
    }

    @Override
    public <T> T getObject(int parmIdx, Class<T> type) throws SQLException {
        return hidden.getObject(parmIdx, type);
    }

    @Override
    public <T> T getObject(String parmName, Class<T> type) throws SQLException {
        return hidden.getObject(parmName, type);
    }

    @Override
    public Ref getRef(int parmIdx) throws SQLException {
        return hidden.getRef(parmIdx);
    }

    @Override
    public Ref getRef(String parmName) throws SQLException {
        return hidden.getRef(parmName);
    }

    @Override
    public ResultSet getResultSet() throws UcanaccessSQLException {
        return tryCatch(hidden::getResultSet);
    }

    @Override
    public RowId getRowId(int parmIdx) throws SQLException {
        return hidden.getRowId(parmIdx);
    }

    @Override
    public RowId getRowId(String parmName) throws SQLException {
        return hidden.getRowId(parmName);
    }

    @Override
    public short getShort(int parmIdx) throws SQLException {
        return hidden.getShort(parmIdx);
    }

    @Override
    public short getShort(String parmName) throws SQLException {
        return hidden.getShort(parmName);
    }

    @Override
    public SQLXML getSQLXML(int parmIdx) throws SQLException {
        return hidden.getSQLXML(parmIdx);
    }

    @Override
    public SQLXML getSQLXML(String parmName) throws SQLException {
        return hidden.getSQLXML(parmName);
    }

    @Override
    public String getString(int parmIdx) throws SQLException {
        return hidden.getString(parmIdx);
    }

    @Override
    public String getString(String parmName) throws SQLException {
        return hidden.getString(parmName);
    }

    @Override
    public Time getTime(int parmIdx, Calendar cal) throws SQLException {
        return hidden.getTime(parmIdx, cal);
    }

    @Override
    public Time getTime(int parmIdx) throws SQLException {
        return hidden.getTime(parmIdx);
    }

    @Override
    public Time getTime(String parmName, Calendar cal) throws SQLException {
        return hidden.getTime(parmName, cal);
    }

    @Override
    public Time getTime(String parmName) throws SQLException {
        return hidden.getTime(parmName);
    }

    @Override
    public Timestamp getTimestamp(int parmIdx, Calendar cal) throws SQLException {
        return hidden.getTimestamp(parmIdx, cal);
    }

    @Override
    public Timestamp getTimestamp(int parmIdx) throws SQLException {
        return hidden.getTimestamp(parmIdx);
    }

    @Override
    public Timestamp getTimestamp(String parmName, Calendar cal) throws SQLException {
        return hidden.getTimestamp(parmName, cal);
    }

    @Override
    public Timestamp getTimestamp(String parmName) throws SQLException {
        return hidden.getTimestamp(parmName);
    }

    @Override
    public URL getURL(int parmIdx) throws SQLException {
        return hidden.getURL(parmIdx);
    }

    @Override
    public URL getURL(String parmName) throws SQLException {
        return hidden.getURL(parmName);
    }

    @Override
    public void registerOutParameter(int parmIdx, int sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(parmIdx, sqlType, scale);
    }

    @Override
    public void registerOutParameter(int parmIdx, int sqlType, String typeName) throws SQLException {
        hidden.registerOutParameter(parmIdx, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(int parmIdx, int sqlType) throws SQLException {
        hidden.registerOutParameter(parmIdx, sqlType);
    }

    @Override
    public void registerOutParameter(String parmName, int sqlType, int scale) throws SQLException {
        hidden.registerOutParameter(parmName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parmName, int sqlType, String typeName) throws SQLException {
        hidden.registerOutParameter(parmName, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parmName, int sqlType) throws SQLException {
        hidden.registerOutParameter(parmName, sqlType);
    }

    @Override
    public void setAsciiStream(String parmName, InputStream x, int length) throws SQLException {
        hidden.setAsciiStream(parmName, x, length);
    }

    @Override
    public void setAsciiStream(String parmName, InputStream x, long length) throws SQLException {
        hidden.setAsciiStream(parmName, x, length);
    }

    @Override
    public void setAsciiStream(String parmName, InputStream x) throws SQLException {
        hidden.setAsciiStream(parmName, x);
    }

    @Override
    public void setBigDecimal(String parmName, BigDecimal x) throws SQLException {
        hidden.setBigDecimal(parmName, x);
    }

    @Override
    public void setBinaryStream(String parmName, InputStream x, int length) throws SQLException {
        hidden.setBinaryStream(parmName, x, length);
    }

    @Override
    public void setBinaryStream(String parmName, InputStream x, long length) throws SQLException {
        hidden.setBinaryStream(parmName, x, length);
    }

    @Override
    public void setBinaryStream(String parmName, InputStream x) throws SQLException {
        hidden.setBinaryStream(parmName, x);
    }

    @Override
    public void setBlob(String parmName, Blob x) throws SQLException {
        hidden.setBlob(parmName, x);
    }

    @Override
    public void setBlob(String parmName, InputStream inputStream, long length) throws SQLException {
        hidden.setBlob(parmName, inputStream, length);
    }

    @Override
    public void setBlob(String parmName, InputStream inputStream) throws SQLException {
        hidden.setBlob(parmName, inputStream);
    }

    @Override
    public void setBoolean(String parmName, boolean x) throws SQLException {
        hidden.setBoolean(parmName, x);
    }

    @Override
    public void setByte(String parmName, byte x) throws SQLException {
        hidden.setByte(parmName, x);
    }

    @Override
    public void setBytes(String parmName, byte[] x) throws SQLException {
        hidden.setBytes(parmName, x);
    }

    @Override
    public void setCharacterStream(String parmName, Reader reader, int length) throws SQLException {
        hidden.setCharacterStream(parmName, reader, length);
    }

    @Override
    public void setCharacterStream(String parmName, Reader reader, long length) throws SQLException {
        hidden.setCharacterStream(parmName, reader, length);
    }

    @Override
    public void setCharacterStream(String parmName, Reader reader) throws SQLException {
        hidden.setCharacterStream(parmName, reader);
    }

    @Override
    public void setClob(String parmName, Clob x) throws SQLException {
        hidden.setClob(parmName, x);
    }

    @Override
    public void setClob(String parmName, Reader reader, long length) throws SQLException {
        hidden.setClob(parmName, reader, length);
    }

    @Override
    public void setClob(String parmName, Reader reader) throws SQLException {
        hidden.setClob(parmName, reader);
    }

    @Override
    public void setDate(String parmName, Date x, Calendar cal) throws SQLException {
        hidden.setDate(parmName, x, cal);
    }

    @Override
    public void setDate(String parmName, Date x) throws SQLException {
        hidden.setDate(parmName, x);
    }

    @Override
    public void setDouble(String parmName, double x) throws SQLException {
        hidden.setDouble(parmName, x);
    }

    @Override
    public void setFloat(String parmName, float x) throws SQLException {
        hidden.setFloat(parmName, x);
    }

    @Override
    public void setInt(int parmIdx, int arg1) throws UcanaccessSQLException {
        tryCatch(() -> hidden.setInt(parmIdx, arg1));
    }

    @Override
    public void setInt(String parmName, int x) throws SQLException {
        hidden.setInt(parmName, x);
    }

    @Override
    public void setLong(String parmName, long x) throws SQLException {
        hidden.setLong(parmName, x);
    }

    @Override
    public void setNCharacterStream(String parmName, Reader value, long length) throws SQLException {
        hidden.setNCharacterStream(parmName, value, length);
    }

    @Override
    public void setNCharacterStream(String parmName, Reader value) throws SQLException {
        hidden.setNCharacterStream(parmName, value);
    }

    @Override
    public void setNClob(String parmName, NClob value) throws SQLException {
        hidden.setNClob(parmName, value);
    }

    @Override
    public void setNClob(String parmName, Reader reader, long length) throws SQLException {
        hidden.setNClob(parmName, reader, length);
    }

    @Override
    public void setNClob(String parmName, Reader reader) throws SQLException {
        hidden.setNClob(parmName, reader);
    }

    @Override
    public void setNString(String parmName, String value) throws SQLException {
        hidden.setNString(parmName, value);
    }

    @Override
    public void setNull(String parmName, int sqlType, String typeName) throws SQLException {
        hidden.setNull(parmName, sqlType, typeName);
    }

    @Override
    public void setNull(String parmName, int sqlType) throws SQLException {
        hidden.setNull(parmName, sqlType);
    }

    @Override
    public void setObject(String parmName, Object x, int targetSqlType, int scale) throws SQLException {
        hidden.setObject(parmName, x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parmName, Object x, int targetSqlType) throws SQLException {
        hidden.setObject(parmName, x, targetSqlType);
    }

    @Override
    public void setObject(String parmName, Object x) throws SQLException {
        hidden.setObject(parmName, x);
    }

    @Override
    public void setRowId(String parmName, RowId x) throws SQLException {
        hidden.setRowId(parmName, x);
    }

    @Override
    public void setSQLXML(String parmName, SQLXML xmlObject) throws SQLException {
        hidden.setSQLXML(parmName, xmlObject);
    }

    @Override
    public void setString(String parmName, String x) throws SQLException {
        hidden.setString(parmName, x);
    }

    @Override
    public void setTime(String parmName, Time x, Calendar cal) throws SQLException {
        hidden.setTime(parmName, x, cal);
    }

    @Override
    public void setTime(String parmName, Time x) throws SQLException {
        hidden.setTime(parmName, x);
    }

    @Override
    public void setTimestamp(String parmName, Timestamp x, Calendar cal) throws SQLException {
        hidden.setTimestamp(parmName, x, cal);
    }

    @Override
    public void setTimestamp(String parmName, Timestamp x) throws SQLException {
        hidden.setTimestamp(parmName, x);
    }

    @Override
    public void setURL(String parmName, URL val) throws SQLException {
        hidden.setURL(parmName, val);
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
