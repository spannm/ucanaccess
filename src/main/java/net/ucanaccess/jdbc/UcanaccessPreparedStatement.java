package net.ucanaccess.jdbc;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

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

@SuppressWarnings("java:S1192")
public class UcanaccessPreparedStatement extends UcanaccessStatement implements PreparedStatement {

    private PreparedStatement                  wrapped;
    private String                             sql;
    private final Map<Integer, ParameterReset> memento = new HashMap<>();

    public UcanaccessPreparedStatement(NormalizedSQL _nsql, PreparedStatement _hidden, UcanaccessConnection _conn) throws SQLException {
        super(_hidden, _conn);
        sql = _nsql.getSql();
        setAliases(_nsql.getAliases());
        wrapped = _hidden;
        if (_hidden == null) {
            super.wrapped = _conn.createStatement();
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
        Class<?>[] ac = new Class<?>[args.length];
        ac[0] = Integer.TYPE;
        System.arraycopy(argClasses, 0, ac, 1, ac.length - 1);
        memento.put((Integer) args[0], new ParameterReset(methodName, ac, args));
    }

    private void parametersReset() {
        for (ParameterReset pr : memento.values()) {
            pr.execute();
        }
    }

    private Reader markableReader(Reader r) throws UcanaccessSQLException {
        return markableReader(r, -1);
    }

    private Reader markableReader(Reader r, long l) throws UcanaccessSQLException {
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
        return tryCatch(() -> {
            int rd;
            while ((rd = r.read(cb)) >= 0) {
                sb.append(Arrays.copyOf(cb, rd));
                if (l >= 0) {
                    break;
                }
            }
            StringReader sr = new StringReader(sb.toString());
            sr.mark(1000000);
            return sr;
        });
    }

    private InputStream markableInputStream(InputStream is) throws UcanaccessSQLException {
        return markableInputStream(is, -1);
    }

    private InputStream markableInputStream(InputStream is, long l) throws UcanaccessSQLException {
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
            throw new UcanaccessSQLException(_ex);
        }
    }

    private void resetReader(Reader r) throws UcanaccessSQLException {
        tryCatch(r::reset);
    }

    private void resetInputStream(InputStream is) throws UcanaccessSQLException {
        tryCatch(is::reset);
    }

    private void preprocess() throws UcanaccessSQLException {
        if (SQLConverter.hasIdentity(sql)) {
            sql = SQLConverter.preprocess(sql, getConnection().getLastGeneratedKey());
            reset();
        }

    }

    @Override
    public void addBatch() throws UcanaccessSQLException {
        tryCatch(() -> wrapped.addBatch());
    }

    @Override
    public void clearParameters() throws UcanaccessSQLException {
        tryCatch(() -> {
            memento.clear();
            wrapped.clearParameters();
        });
    }

    @Override
    public boolean execute() throws UcanaccessSQLException {
        return tryCatch(() -> {
            if (wrapped == null) {
                return super.wrapped.execute(sql);
            }
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new Execute(this).execute();
        });
    }

    @Override
    public ResultSet executeQuery() throws UcanaccessSQLException {
        return tryCatch(() -> {
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new UcanaccessResultSet(wrapped.executeQuery(), this);
        });
    }

    @Override
    public int executeUpdate() throws UcanaccessSQLException {
        return tryCatch(() -> {
            if (wrapped == null) {
                return super.wrapped.executeUpdate(sql);
            }
            preprocess();
            getConnection().setCurrentStatement(this);
            checkLastModified();
            return new ExecuteUpdate(this).execute();
        });
    }

    @Override
    public ResultSetMetaData getMetaData() throws UcanaccessSQLException {
        return tryCatch(wrapped::getMetaData);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws UcanaccessSQLException {
        return tryCatch(wrapped::getParameterMetaData);
    }

    @Override
    public void setArray(int _parmIdx, Array array) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setArray", new Class<?>[] {Array.class}, _parmIdx, array);
            wrapped.setArray(_parmIdx, array);
        });
    }

    @Override
    public void setAsciiStream(int _parmIdx, InputStream is) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class}, _parmIdx, is);
            wrapped.setAsciiStream(_parmIdx, is);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAsciiStream(int _parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class, Integer.TYPE}, _parmIdx, is, length);
            wrapped.setAsciiStream(_parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAsciiStream(int _parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class, Long.TYPE}, _parmIdx, is, length);
            wrapped.setAsciiStream(_parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setBigDecimal(int _parmIdx, BigDecimal dec) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBigDecimal", new Class<?>[] {BigDecimal.class}, _parmIdx, dec);
            wrapped.setBigDecimal(_parmIdx, dec);
        });
    }

    @Override
    public void setBinaryStream(int _parmIdx, InputStream is) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class}, _parmIdx, is);
            wrapped.setBinaryStream(_parmIdx, is);
        });
    }

    @Override
    public void setBinaryStream(int _parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class, Integer.TYPE}, _parmIdx, is, length);
            wrapped.setBinaryStream(_parmIdx, is, length);
        });
    }

    @Override
    public void setBinaryStream(int _parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class, Long.TYPE}, _parmIdx, is, length);
            wrapped.setBinaryStream(_parmIdx, is, length);
        });
    }

    @Override
    public void setBlob(int _parmIdx, Blob blob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {Blob.class}, _parmIdx, blob);
            wrapped.setBlob(_parmIdx, blob);
        });
    }

    @Override
    public void setBlob(int _parmIdx, InputStream is) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {InputStream.class}, _parmIdx, is);
            wrapped.setBlob(_parmIdx, is);
        });
    }

    @Override
    public void setBlob(int _parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {InputStream.class, Long.TYPE}, _parmIdx, is, length);
            wrapped.setBlob(_parmIdx, is, length);
        });
    }

    @Override
    public void setBoolean(int _parmIdx, boolean bool) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBoolean", new Class<?>[] {Boolean.TYPE}, _parmIdx, bool);
            wrapped.setBoolean(_parmIdx, bool);
        });
    }

    @Override
    public void setByte(int _parmIdx, byte b) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setByte", new Class<?>[] {Byte.TYPE}, _parmIdx, b);
            wrapped.setByte(_parmIdx, b);
        });
    }

    @Override
    public void setBytes(int _parmIdx, byte[] bytes) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBytes", new Class<?>[] {byte[].class}, _parmIdx, bytes);
            wrapped.setBytes(_parmIdx, bytes);
        });
    }

    @Override
    public void setCharacterStream(int _parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class}, _parmIdx, reader);

            wrapped.setCharacterStream(_parmIdx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCharacterStream(int _parmIdx, Reader reader, int length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class, Integer.TYPE}, _parmIdx, reader, length);
            wrapped.setCharacterStream(_parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCharacterStream(int _parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class, Long.TYPE}, _parmIdx, reader, length);
            wrapped.setCharacterStream(_parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setClob(int _parmIdx, Clob clob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setClob", new Class<?>[] {Clob.class}, _parmIdx, clob);
            wrapped.setClob(_parmIdx, clob);
        });
    }

    @Override
    public void setClob(int _parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setClob", new Class<?>[] {Reader.class}, _parmIdx, reader);
            wrapped.setClob(_parmIdx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setClob(int _parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setClob", new Class<?>[] {Reader.class, Long.TYPE}, _parmIdx, reader, length);
            wrapped.setClob(_parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setCursorName(String _name) throws UcanaccessSQLException {
        tryCatch(() -> wrapped.setCursorName(_name));
    }

    @Override
    public void setDate(int _parmIdx, Date date) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDate", new Class<?>[] {Date.class}, _parmIdx, date);
            wrapped.setDate(_parmIdx, date);
        });
    }

    @Override
    public void setDate(int _parmIdx, Date date, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDate", new Class<?>[] {Date.class, Calendar.class}, _parmIdx, date, cal);
            wrapped.setDate(_parmIdx, date, cal);
        });
    }

    @Override
    public void setDouble(int _parmIdx, double d) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDouble", new Class<?>[] {Double.TYPE}, _parmIdx, d);
            wrapped.setDouble(_parmIdx, d);
        });
    }

    @Override
    public void setFloat(int _parmIdx, float f) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setFloat", new Class<?>[] {Float.TYPE}, _parmIdx, f);
            wrapped.setBigDecimal(_parmIdx, new BigDecimal(Float.toString(f)));
        });
    }

    @Override
    public void setInt(int _parmIdx, int i) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setInt", new Class<?>[] {Integer.TYPE}, _parmIdx, i);
            wrapped.setInt(_parmIdx, i);
        });
    }

    @Override
    public void setLong(int _parmIdx, long l) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setLong", new Class<?>[] {Long.TYPE}, _parmIdx, l);
            wrapped.setLong(_parmIdx, l);
        });
    }

    @Override
    public void setNCharacterStream(int _parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNCharacterStream", new Class<?>[] {Reader.class}, _parmIdx, reader);
            wrapped.setNCharacterStream(_parmIdx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNCharacterStream(int _parmIdx, Reader reader, long l) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, l);
            addMementoEntry("setNCharacterStream", new Class<?>[] {Reader.class, Long.TYPE}, _parmIdx, reader, l);
            wrapped.setNCharacterStream(_parmIdx, reader, l);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNClob(int _parmIdx, NClob nclob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNClob", new Class<?>[] {NClob.class}, _parmIdx, nclob);
            wrapped.setNClob(_parmIdx, nclob);
        });
    }

    @Override
    public void setNClob(int _parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNClob", new Class<?>[] {Reader.class}, _parmIdx, reader);
            wrapped.setNClob(_parmIdx, reader);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNClob(int _parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setNClob", new Class<?>[] {Reader.class, Long.TYPE}, _parmIdx, reader, length);
            wrapped.setNClob(_parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNString(int _parmIdx, String string) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNString", new Class<?>[] {String.class}, _parmIdx, string);
            wrapped.setNString(_parmIdx, string);
        });
    }

    @Override
    public void setNull(int _parmIdx, int sqlt) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNull", new Class<?>[] {Integer.TYPE}, _parmIdx, sqlt);
            wrapped.setNull(_parmIdx, sqlt);
        });
    }

    @Override
    public void setNull(int _parmIdx, int sqlt, String tn) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNull", new Class<?>[] {Integer.TYPE, String.class}, _parmIdx, sqlt, tn);
            wrapped.setNull(_parmIdx, sqlt, tn);
        });
    }

    private Object mapLocalTimeToLocalDateTime(Object _x) {
        if (_x instanceof LocalTime) {
            return ((LocalTime) _x).atDate(LocalDate.of(1899, 12, 30));
        }
        return _x;
    }

    private Object mapToBlob(Object x) throws UcanaccessSQLException {
        return tryCatch(() -> x instanceof File ? UcanaccessBlob.createBlob((File) x, getConnection()) : x);
    }

    @Override
    public void setObject(int _parmIdx, Object x) throws UcanaccessSQLException {
        x = mapToBlob(mapLocalTimeToLocalDateTime(x));
        try {
            if (x instanceof Float) {
                setFloat(_parmIdx, (Float) x);
            } else {
                addMementoEntry("setObject", new Class<?>[] {Object.class}, _parmIdx, x);
                wrapped.setObject(_parmIdx, x);
            }
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setObject(int _parmIdx, Object _obj, int tsqlt) throws UcanaccessSQLException {
        Object obj = mapToBlob(mapLocalTimeToLocalDateTime(_obj));
        tryCatch(() -> {
            addMementoEntry("setObject", new Class<?>[] {Object.class, Integer.TYPE}, _parmIdx, obj, tsqlt);
            wrapped.setObject(_parmIdx, obj, tsqlt);
        });
    }

    @Override
    public void setObject(int _parmIdx, Object _obj, int tsqlt, int sol) throws UcanaccessSQLException {
        Object obj = mapToBlob(mapLocalTimeToLocalDateTime(_obj));
        tryCatch(() -> {
            addMementoEntry("setObject", new Class<?>[] {Object.class, Integer.TYPE, Integer.TYPE}, _parmIdx, obj, tsqlt, sol);
            wrapped.setObject(_parmIdx, obj, tsqlt, sol);
        });
    }

    @Override
    public void setRef(int _parmIdx, Ref ref) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setRef", new Class<?>[] {Ref.class}, _parmIdx, ref);
            wrapped.setRef(_parmIdx, ref);
        });
    }

    @Override
    public void setRowId(int _parmIdx, RowId rowId) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setRowId", new Class<?>[] {RowId.class}, _parmIdx, rowId);
            wrapped.setRowId(_parmIdx, rowId);
        });
    }

    @Override
    public void setShort(int _parmIdx, short sht) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setShort", new Class<?>[] {Short.TYPE}, _parmIdx, sht);
            wrapped.setShort(_parmIdx, sht);
        });
    }

    @Override
    public void setSQLXML(int _parmIdx, SQLXML sx) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setSQLXML", new Class<?>[] {SQLXML.class}, _parmIdx, sx);
            wrapped.setSQLXML(_parmIdx, sx);
        });
    }

    @Override
    public void setString(int _parmIdx, String string) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setString", new Class<?>[] {String.class}, _parmIdx, string);
            wrapped.setString(_parmIdx, string);
        });
    }

    @Override
    public void setTime(int _parmIdx, Time time) throws UcanaccessSQLException {
        tryCatch(() -> {
            Calendar cl = Calendar.getInstance();
            cl.setTime(time);
            cl.set(1899, 11, 30);
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, _parmIdx, ts);
            wrapped.setTimestamp(_parmIdx, ts);
        });
    }

    @Override
    public void setTime(int _parmIdx, Time time, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            Calendar cl = Calendar.getInstance();
            cal.setTime(time);
            cl.set(1899, 11, 30, cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, _parmIdx, ts);
            wrapped.setTimestamp(_parmIdx, ts);
        });
    }

    @Override
    public void setTimestamp(int _parmIdx, Timestamp ts) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, _parmIdx, ts);
            wrapped.setTimestamp(_parmIdx, ts);
        });
    }

    @Override
    public void setTimestamp(int _parmIdx, Timestamp ts, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class, Calendar.class}, _parmIdx, ts, cal);
            wrapped.setTimestamp(_parmIdx, ts, cal);
        });
    }

    /**
     * @deprecated Use {@code setCharacterStream}
     */
    @Override
    @Deprecated
    public void setUnicodeStream(int _parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setUnicodeStream", new Class<?>[] {InputStream.class, Integer.TYPE}, _parmIdx, is, length);
            wrapped.setUnicodeStream(_parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setURL(int _parmIdx, URL url) throws UcanaccessSQLException {
        tryCatch(() -> {
            String arg = "#" + url.toString() + "#";
            addMementoEntry("setString", new Class<?>[] {String.class}, _parmIdx, arg);
            wrapped.setString(_parmIdx, arg);
        });
    }

    @Override
    public <T> T unwrap(Class<T> _iface) throws UcanaccessSQLException {
        return tryCatch(() -> wrapped.unwrap(_iface));
    }

    @Override
    protected void reset() throws UcanaccessSQLException {
        if (wrapped == null) {
            return;
        }
        PreparedStatement old = wrapped;
        wrapped = tryCatch(() -> getConnection().getHSQLDBConnection().prepareStatement(sql,
            wrapped.getResultSetType(), wrapped.getResultSetConcurrency(), wrapped.getResultSetHoldability()));
        reset(wrapped);
        parametersReset();
        tryCatch(old::close);
    }

}
