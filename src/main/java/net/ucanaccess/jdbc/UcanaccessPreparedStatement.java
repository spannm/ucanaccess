package net.ucanaccess.jdbc;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.io.*;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class UcanaccessPreparedStatement extends UcanaccessStatement implements PreparedStatement {

    private PreparedStatement            wrapped;
    private String                       sql;
    private Map<Integer, ParameterReset> memento = new HashMap<>();

    public UcanaccessPreparedStatement(NormalizedSQL nsql, PreparedStatement hidden, UcanaccessConnection connection)
        throws SQLException {
        super(hidden, connection);
        sql = nsql.getSql();
        setAliases(nsql.getAliases());
        wrapped = hidden;
        if (hidden == null) {
            super.wrapped = connection.createStatement();
        }
    }

    public UcanaccessPreparedStatement(String _sql, UcanaccessConnection _connection) throws SQLException {
        super(null, _connection);
        sql = _sql;
        super.wrapped = _connection.createStatement();
    }

    private final class ParameterReset {
        private final String     methodName;
        private final Object[]   args;
        private final Class<?>[] argClasses;

        private ParameterReset(String _methodName, Class<?>[] _argClasses, Object... _args) {
            methodName = _methodName;
            args = _args;
            argClasses = _argClasses;
        }

        void execute() {
            Try.catching(() -> {
                Method mth = PreparedStatement.class.getDeclaredMethod(methodName, argClasses);
                mth.invoke(wrapped, args);
                if (args[1] instanceof StringReader) {
                    StringReader sr = (StringReader) args[1];
                    sr.reset();
                }
                if (args[1] instanceof InputStream
                    && ("setAsciiStream".equals(methodName) || "setUnicodeStream".equals(methodName))) {
                    ((InputStream) args[1]).reset();
                }
            }).orThrow(UcanaccessRuntimeException::new);
        }

    }

    private void addMementoEntry(String methodName, Class<?>[] argClasses, Object... args) {
        Class<?>[] ac = new Class[args.length];
        ac[0] = Integer.TYPE;
        for (int y = 1; y < ac.length; y++) {
            ac[y] = argClasses[y - 1];
        }
        memento.put((Integer) args[0], new ParameterReset(methodName, ac, args));
    }

    private void parametersReset() {
        for (ParameterReset pr : memento.values()) {
            pr.execute();
        }
    }

    private Reader markableReader(Reader r) throws SQLException {
        return markableReader(r, -1);
    }

    private Reader markableReader(Reader r, long l) throws SQLException {
        if (r.markSupported() && l < 0) {
            boolean marked = true;
            try {
                r.mark(1000000);
            } catch (IOException _ex) {
                marked = false;
            }
            if (marked) {
                return r;
            }
        }

        StringBuilder sb = new StringBuilder();
        int dim = l >= 0 ? (int) l : 4096;
        char[] cb = new char[dim];
        int rd;
        try {

            while ((rd = r.read(cb)) >= 0) {
                sb.append(Arrays.copyOf(cb, rd));
                if (l >= 0) {
                    break;
                }
            }
            StringReader sr = new StringReader(sb.toString());
            sr.mark(1000000);
            return sr;
        } catch (IOException _ex) {
            throw new SQLException(_ex);
        }
    }

    private InputStream markableInputStream(InputStream is) throws SQLException {
        return markableInputStream(is, -1);
    }

    private InputStream markableInputStream(InputStream is, long l) throws SQLException {
        if (is.markSupported() && l < 0) {
            is.mark(1000000);
            return is;
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int dim = l >= 0 ? (int) l : 4096;
        byte[] buffer = new byte[dim];
        int rd;
        try {
            while ((rd = is.read(buffer)) >= 0) {
                bos.write(buffer, 0, rd);
                if (l >= 0) {
                    break;
                }
            }
            bos.flush();
            ByteArrayInputStream ir = new ByteArrayInputStream(bos.toByteArray());
            ir.mark(1000000);
            return ir;
        } catch (IOException _ex) {
            throw new SQLException(_ex);
        }
    }

    private void resetReader(Reader r) throws SQLException {
        try {
            r.reset();
        } catch (IOException _ex) {
            throw new SQLException(_ex);
        }
    }

    private void resetInputStream(InputStream is) throws SQLException {
        try {
            is.reset();
        } catch (IOException _ex) {
            throw new SQLException(_ex);
        }
    }

    private void preprocess() throws SQLException {
        if (SQLConverter.hasIdentity(sql)) {
            sql =
                SQLConverter.preprocess(sql, getConnection().getLastGeneratedKey());
            reset();
        }

    }

    @Override
    public void addBatch() throws SQLException {
        try {
            wrapped.addBatch();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            memento.clear();
            wrapped.clearParameters();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            if (wrapped == null) {
                return super.wrapped.execute(sql);
            }
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new Execute(this).execute();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new UcanaccessResultSet(wrapped.executeQuery(), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            if (wrapped == null) {
                return super.wrapped.executeUpdate(sql);
            }
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new ExecuteUpdate(this).execute();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            return wrapped.getMetaData();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            return wrapped.getParameterMetaData();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setArray(int idx, Array array) throws SQLException {
        try {
            addMementoEntry("setArray", new Class[] {Array.class}, idx, array);
            wrapped.setArray(idx, array);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAsciiStream(int idx, InputStream is) throws SQLException {
        try {
            is = markableInputStream(is);
            addMementoEntry("setAsciiStream", new Class[] {InputStream.class}, idx, is);
            wrapped.setAsciiStream(idx, is);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAsciiStream(int idx, InputStream is, int length) throws SQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class[] {InputStream.class, Integer.TYPE}, idx, is, length);
            wrapped.setAsciiStream(idx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAsciiStream(int idx, InputStream is, long length) throws SQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class[] {InputStream.class, Long.TYPE}, idx, is, length);
            wrapped.setAsciiStream(idx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBigDecimal(int idx, BigDecimal dec) throws SQLException {
        try {
            addMementoEntry("setBigDecimal", new Class[] {BigDecimal.class}, idx, dec);
            wrapped.setBigDecimal(idx, dec);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBinaryStream(int idx, InputStream is) throws SQLException {
        try {
            addMementoEntry("setBinaryStream", new Class[] {InputStream.class}, idx, is);
            wrapped.setBinaryStream(idx, is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBinaryStream(int idx, InputStream is, int length) throws SQLException {
        try {
            addMementoEntry("setBinaryStream", new Class[] {InputStream.class, Integer.TYPE}, idx, is, length);
            wrapped.setBinaryStream(idx, is, length);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBinaryStream(int idx, InputStream is, long length) throws SQLException {
        try {
            addMementoEntry("setBinaryStream", new Class[] {InputStream.class, Long.TYPE}, idx, is, length);
            wrapped.setBinaryStream(idx, is, length);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBlob(int idx, Blob blob) throws SQLException {
        try {
            addMementoEntry("setBlob", new Class[] {Blob.class}, idx, blob);
            wrapped.setBlob(idx, blob);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBlob(int idx, InputStream is) throws SQLException {
        try {
            addMementoEntry("setBlob", new Class[] {InputStream.class}, idx, is);
            wrapped.setBlob(idx, is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBlob(int idx, InputStream is, long length) throws SQLException {
        try {
            addMementoEntry("setBlob", new Class[] {InputStream.class, Long.TYPE}, idx, is, length);
            wrapped.setBlob(idx, is, length);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBoolean(int idx, boolean bool) throws SQLException {
        try {
            addMementoEntry("setBoolean", new Class[] {Boolean.TYPE}, idx, bool);
            wrapped.setBoolean(idx, bool);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setByte(int idx, byte b) throws SQLException {
        try {
            addMementoEntry("setByte", new Class[] {Byte.TYPE}, idx, b);
            wrapped.setByte(idx, b);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBytes(int idx, byte[] bytes) throws SQLException {
        try {
            addMementoEntry("setBytes", new Class[] {byte[].class}, idx, bytes);
            wrapped.setBytes(idx, bytes);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCharacterStream(int idx, Reader reader) throws SQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setCharacterStream", new Class[] {Reader.class}, idx, reader);

            wrapped.setCharacterStream(idx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCharacterStream(int idx, Reader reader, int length) throws SQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class[] {Reader.class, Integer.TYPE}, idx, reader, length);
            wrapped.setCharacterStream(idx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCharacterStream(int idx, Reader reader, long length) throws SQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class[] {Reader.class, Long.TYPE}, idx, reader, length);
            wrapped.setCharacterStream(idx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setClob(int idx, Clob clob) throws SQLException {
        try {
            addMementoEntry("setClob", new Class[] {Clob.class}, idx, clob);
            wrapped.setClob(idx, clob);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setClob(int idx, Reader reader) throws SQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setClob", new Class[] {Reader.class}, idx, reader);
            wrapped.setClob(idx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setClob(int idx, Reader reader, long length) throws SQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setClob", new Class[] {Reader.class, Long.TYPE}, idx, reader, length);
            wrapped.setClob(idx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        try {
            wrapped.setCursorName(name);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setDate(int idx, Date date) throws SQLException {
        try {
            addMementoEntry("setDate", new Class[] {Date.class}, idx, date);
            wrapped.setDate(idx, date);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setDate(int idx, Date date, Calendar cal) throws SQLException {
        try {
            addMementoEntry("setDate", new Class[] {Date.class, Calendar.class}, idx, date, cal);
            wrapped.setDate(idx, date, cal);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setDouble(int idx, double d) throws SQLException {
        try {
            addMementoEntry("setDouble", new Class[] {Double.TYPE}, idx, d);
            wrapped.setDouble(idx, d);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setFloat(int idx, float f) throws SQLException {
        try {
            addMementoEntry("setFloat", new Class[] {Float.TYPE}, idx, f);
            wrapped.setBigDecimal(idx, new BigDecimal(Float.toString(f)));
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setInt(int idx, int i) throws SQLException {
        try {
            addMementoEntry("setInt", new Class[] {Integer.TYPE}, idx, i);
            wrapped.setInt(idx, i);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setLong(int idx, long l) throws SQLException {
        try {
            addMementoEntry("setLong", new Class[] {Long.TYPE}, idx, l);
            wrapped.setLong(idx, l);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNCharacterStream(int idx, Reader reader) throws SQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNCharacterStream", new Class[] {Reader.class}, idx, reader);
            wrapped.setNCharacterStream(idx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNCharacterStream(int idx, Reader reader, long l) throws SQLException {
        try {
            reader = markableReader(reader, l);
            addMementoEntry("setNCharacterStream", new Class[] {Reader.class, Long.TYPE}, idx, reader, l);
            wrapped.setNCharacterStream(idx, reader, l);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNClob(int idx, NClob nclob) throws SQLException {
        try {
            addMementoEntry("setNClob", new Class[] {NClob.class}, idx, nclob);
            wrapped.setNClob(idx, nclob);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNClob(int idx, Reader reader) throws SQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNClob", new Class[] {Reader.class}, idx, reader);
            wrapped.setNClob(idx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNClob(int idx, Reader reader, long length) throws SQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setNClob", new Class[] {Reader.class, Long.TYPE}, idx, reader, length);
            wrapped.setNClob(idx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNString(int idx, String string) throws SQLException {
        try {
            addMementoEntry("setNString", new Class[] {String.class}, idx, string);
            wrapped.setNString(idx, string);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNull(int idx, int sqlt) throws SQLException {
        try {
            addMementoEntry("setNull", new Class[] {Integer.TYPE}, idx, sqlt);
            wrapped.setNull(idx, sqlt);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNull(int idx, int sqlt, String tn) throws SQLException {
        try {
            addMementoEntry("setNull", new Class[] {Integer.TYPE, String.class}, idx, sqlt, tn);
            wrapped.setNull(idx, sqlt, tn);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    private Object mapLocalTimeToLocalDateTime(Object x) {
        if (x instanceof LocalTime) {
            return ((LocalTime) x).atDate(LocalDate.of(1899, 12, 30));
        }
        return x;
    }

    private Object mapToBlob(Object x) throws SQLException {
        if (x instanceof File) {
            x = UcanaccessBlob.createBlob((File) x, getConnection());
        }
        return x;
    }

    @Override
    public void setObject(int idx, Object x) throws SQLException {
        x = mapToBlob(mapLocalTimeToLocalDateTime(x));
        try {
            if (x instanceof Float) {
                setFloat(idx, (Float) x);
            } else {
                addMementoEntry("setObject", new Class[] {Object.class}, idx, x);
                wrapped.setObject(idx, x);
            }
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setObject(int idx, Object x, int tsqlt) throws SQLException {
        x = mapToBlob(mapLocalTimeToLocalDateTime(x));
        try {
            addMementoEntry("setObject", new Class[] {Object.class, Integer.TYPE}, idx, x, tsqlt);
            wrapped.setObject(idx, x, tsqlt);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setObject(int idx, Object x, int tsqlt, int sol) throws SQLException {
        x = mapToBlob(mapLocalTimeToLocalDateTime(x));
        try {
            addMementoEntry("setObject", new Class[] {Object.class, Integer.TYPE, Integer.TYPE}, idx, x, tsqlt, sol);
            wrapped.setObject(idx, x, tsqlt, sol);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setRef(int idx, Ref ref) throws SQLException {
        try {
            addMementoEntry("setRef", new Class[] {Ref.class}, idx, ref);
            wrapped.setRef(idx, ref);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setRowId(int idx, RowId rowId) throws SQLException {
        try {
            addMementoEntry("setRowId", new Class[] {RowId.class}, idx, rowId);
            wrapped.setRowId(idx, rowId);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setShort(int idx, short sht) throws SQLException {
        try {
            addMementoEntry("setShort", new Class[] {Short.TYPE}, idx, sht);
            wrapped.setShort(idx, sht);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setSQLXML(int idx, SQLXML sx) throws SQLException {
        try {
            addMementoEntry("setSQLXML", new Class[] {SQLXML.class}, idx, sx);
            wrapped.setSQLXML(idx, sx);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setString(int idx, String string) throws SQLException {
        try {
            addMementoEntry("setString", new Class[] {String.class}, idx, string);
            wrapped.setString(idx, string);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTime(int idx, Time time) throws SQLException {
        try {
            Calendar cl = Calendar.getInstance();
            cl.setTime(time);
            cl.set(1899, 11, 30);
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class[] {Timestamp.class}, idx, ts);
            wrapped.setTimestamp(idx, ts);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTime(int idx, Time time, Calendar cal) throws SQLException {
        try {
            Calendar cl = Calendar.getInstance();
            cal.setTime(time);
            cl.set(1899, 11, 30, cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class[] {Timestamp.class}, idx, ts);
            wrapped.setTimestamp(idx, ts);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTimestamp(int idx, Timestamp ts) throws SQLException {
        try {
            addMementoEntry("setTimestamp", new Class[] {Timestamp.class}, idx, ts);
            wrapped.setTimestamp(idx, ts);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTimestamp(int idx, Timestamp ts, Calendar cal) throws SQLException {
        try {
            addMementoEntry("setTimestamp", new Class[] {Timestamp.class, Calendar.class}, idx, ts, cal);
            wrapped.setTimestamp(idx, ts, cal);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int idx, InputStream is, int length) throws SQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setUnicodeStream", new Class[] {InputStream.class, Integer.TYPE}, idx, is, length);
            wrapped.setUnicodeStream(idx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setURL(int idx, URL url) throws SQLException {
        try {
            String arg = "#" + url.toString() + "#";
            addMementoEntry("setString", new Class[] {String.class}, idx, arg);
            wrapped.setString(idx, arg);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    protected void reset() throws SQLException {
        if (wrapped == null) {
            return;
        }
        PreparedStatement old = wrapped;
        wrapped = getConnection().getHSQLDBConnection().prepareStatement(sql,
            wrapped.getResultSetType(), wrapped.getResultSetConcurrency(), wrapped.getResultSetHoldability());
        reset(wrapped);
        parametersReset();
        old.close();
    }

}
