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

public class UcanaccessPreparedStatement extends UcanaccessStatement implements PreparedStatement {

    private PreparedStatement                  wrapped;
    private String                             sql;
    private final Map<Integer, ParameterReset> memento = new HashMap<>();

    public UcanaccessPreparedStatement(NormalizedSQL nsql, PreparedStatement hidden, UcanaccessConnection conn) throws SQLException {
        super(hidden, conn);
        sql = nsql.getSql();
        setAliases(nsql.getAliases());
        wrapped = hidden;
        if (hidden == null) {
            super.wrapped = conn.createStatement();
        }
    }

    public UcanaccessPreparedStatement(String sql, UcanaccessConnection connection) throws SQLException {
        super(null, connection);
        this.sql = sql;
        super.wrapped = connection.createStatement();
    }

    private final class ParameterReset {
        private final String     methodName;
        private final Object[]   args;
        private final Class<?>[] argClasses;

        private ParameterReset(String methodName, Class<?>[] argClasses, Object... args) {
            this.methodName = methodName;
            this.args = args;
            this.argClasses = argClasses;
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
            } catch (IOException ex) {
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
        } catch (IOException ex) {
            throw new UcanaccessSQLException(ex);
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
    public void setArray(int parmIdx, Array array) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setArray", new Class<?>[] {Array.class}, parmIdx, array);
            wrapped.setArray(parmIdx, array);
        });
    }

    @Override
    public void setAsciiStream(int parmIdx, InputStream is) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class}, parmIdx, is);
            wrapped.setAsciiStream(parmIdx, is);
            resetInputStream(is);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setAsciiStream(int parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class, Integer.TYPE}, parmIdx, is, length);
            wrapped.setAsciiStream(parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setAsciiStream(int parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setAsciiStream", new Class<?>[] {InputStream.class, Long.TYPE}, parmIdx, is, length);
            wrapped.setAsciiStream(parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setBigDecimal(int parmIdx, BigDecimal dec) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBigDecimal", new Class<?>[] {BigDecimal.class}, parmIdx, dec);
            wrapped.setBigDecimal(parmIdx, dec);
        });
    }

    @Override
    public void setBinaryStream(int parmIdx, InputStream is) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class}, parmIdx, is);
            wrapped.setBinaryStream(parmIdx, is);
        });
    }

    @Override
    public void setBinaryStream(int parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class, Integer.TYPE}, parmIdx, is, length);
            wrapped.setBinaryStream(parmIdx, is, length);
        });
    }

    @Override
    public void setBinaryStream(int parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBinaryStream", new Class<?>[] {InputStream.class, Long.TYPE}, parmIdx, is, length);
            wrapped.setBinaryStream(parmIdx, is, length);
        });
    }

    @Override
    public void setBlob(int parmIdx, Blob blob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {Blob.class}, parmIdx, blob);
            wrapped.setBlob(parmIdx, blob);
        });
    }

    @Override
    public void setBlob(int parmIdx, InputStream is) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {InputStream.class}, parmIdx, is);
            wrapped.setBlob(parmIdx, is);
        });
    }

    @Override
    public void setBlob(int parmIdx, InputStream is, long length) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBlob", new Class<?>[] {InputStream.class, Long.TYPE}, parmIdx, is, length);
            wrapped.setBlob(parmIdx, is, length);
        });
    }

    @Override
    public void setBoolean(int parmIdx, boolean bool) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBoolean", new Class<?>[] {Boolean.TYPE}, parmIdx, bool);
            wrapped.setBoolean(parmIdx, bool);
        });
    }

    @Override
    public void setByte(int parmIdx, byte b) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setByte", new Class<?>[] {Byte.TYPE}, parmIdx, b);
            wrapped.setByte(parmIdx, b);
        });
    }

    @Override
    public void setBytes(int parmIdx, byte[] bytes) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setBytes", new Class<?>[] {byte[].class}, parmIdx, bytes);
            wrapped.setBytes(parmIdx, bytes);
        });
    }

    @Override
    public void setCharacterStream(int parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class}, parmIdx, reader);

            wrapped.setCharacterStream(parmIdx, reader);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setCharacterStream(int parmIdx, Reader reader, int length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class, Integer.TYPE}, parmIdx, reader, length);
            wrapped.setCharacterStream(parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setCharacterStream(int parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setCharacterStream", new Class<?>[] {Reader.class, Long.TYPE}, parmIdx, reader, length);
            wrapped.setCharacterStream(parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setClob(int parmIdx, Clob clob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setClob", new Class<?>[] {Clob.class}, parmIdx, clob);
            wrapped.setClob(parmIdx, clob);
        });
    }

    @Override
    public void setClob(int parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setClob", new Class<?>[] {Reader.class}, parmIdx, reader);
            wrapped.setClob(parmIdx, reader);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setClob(int parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setClob", new Class<?>[] {Reader.class, Long.TYPE}, parmIdx, reader, length);
            wrapped.setClob(parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setCursorName(String name) throws UcanaccessSQLException {
        tryCatch(() -> wrapped.setCursorName(name));
    }

    @Override
    public void setDate(int parmIdx, Date date) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDate", new Class<?>[] {Date.class}, parmIdx, date);
            wrapped.setDate(parmIdx, date);
        });
    }

    @Override
    public void setDate(int parmIdx, Date date, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDate", new Class<?>[] {Date.class, Calendar.class}, parmIdx, date, cal);
            wrapped.setDate(parmIdx, date, cal);
        });
    }

    @Override
    public void setDouble(int parmIdx, double d) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setDouble", new Class<?>[] {Double.TYPE}, parmIdx, d);
            wrapped.setDouble(parmIdx, d);
        });
    }

    @Override
    public void setFloat(int parmIdx, float f) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setFloat", new Class<?>[] {Float.TYPE}, parmIdx, f);
            wrapped.setBigDecimal(parmIdx, new BigDecimal(Float.toString(f)));
        });
    }

    @Override
    public void setInt(int parmIdx, int i) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setInt", new Class<?>[] {Integer.TYPE}, parmIdx, i);
            wrapped.setInt(parmIdx, i);
        });
    }

    @Override
    public void setLong(int parmIdx, long l) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setLong", new Class<?>[] {Long.TYPE}, parmIdx, l);
            wrapped.setLong(parmIdx, l);
        });
    }

    @Override
    public void setNCharacterStream(int parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNCharacterStream", new Class<?>[] {Reader.class}, parmIdx, reader);
            wrapped.setNCharacterStream(parmIdx, reader);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setNCharacterStream(int parmIdx, Reader reader, long l) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, l);
            addMementoEntry("setNCharacterStream", new Class<?>[] {Reader.class, Long.TYPE}, parmIdx, reader, l);
            wrapped.setNCharacterStream(parmIdx, reader, l);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setNClob(int parmIdx, NClob nclob) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNClob", new Class<?>[] {NClob.class}, parmIdx, nclob);
            wrapped.setNClob(parmIdx, nclob);
        });
    }

    @Override
    public void setNClob(int parmIdx, Reader reader) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader);
            addMementoEntry("setNClob", new Class<?>[] {Reader.class}, parmIdx, reader);
            wrapped.setNClob(parmIdx, reader);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setNClob(int parmIdx, Reader reader, long length) throws UcanaccessSQLException {
        try {
            reader = markableReader(reader, length);
            addMementoEntry("setNClob", new Class<?>[] {Reader.class, Long.TYPE}, parmIdx, reader, length);
            wrapped.setNClob(parmIdx, reader, length);
            resetReader(reader);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setNString(int parmIdx, String string) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNString", new Class<?>[] {String.class}, parmIdx, string);
            wrapped.setNString(parmIdx, string);
        });
    }

    @Override
    public void setNull(int parmIdx, int sqlt) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNull", new Class<?>[] {Integer.TYPE}, parmIdx, sqlt);
            wrapped.setNull(parmIdx, sqlt);
        });
    }

    @Override
    public void setNull(int parmIdx, int sqlt, String tn) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setNull", new Class<?>[] {Integer.TYPE, String.class}, parmIdx, sqlt, tn);
            wrapped.setNull(parmIdx, sqlt, tn);
        });
    }

    private Object mapLocalTimeToLocalDateTime(Object x) {
        if (x instanceof LocalTime) {
            return ((LocalTime) x).atDate(LocalDate.of(1899, 12, 30));
        }
        return x;
    }

    private Object mapToBlob(Object x) throws UcanaccessSQLException {
        return tryCatch(() -> x instanceof File ? UcanaccessBlob.createBlob((File) x, getConnection()) : x);
    }

    @Override
    public void setObject(int parmIdx, Object x) throws UcanaccessSQLException {
        x = mapToBlob(mapLocalTimeToLocalDateTime(x));
        try {
            if (x instanceof Float) {
                setFloat(parmIdx, (Float) x);
            } else {
                addMementoEntry("setObject", new Class<?>[] {Object.class}, parmIdx, x);
                wrapped.setObject(parmIdx, x);
            }
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setObject(int parmIdx, Object obj, int tsqlt) throws UcanaccessSQLException {
        Object object = mapToBlob(mapLocalTimeToLocalDateTime(obj));
        tryCatch(() -> {
            addMementoEntry("setObject", new Class<?>[] {Object.class, Integer.TYPE}, parmIdx, object, tsqlt);
            wrapped.setObject(parmIdx, object, tsqlt);
        });
    }

    @Override
    public void setObject(int parmIdx, Object object, int tsqlt, int sol) throws UcanaccessSQLException {
        Object obj = mapToBlob(mapLocalTimeToLocalDateTime(object));
        tryCatch(() -> {
            addMementoEntry("setObject", new Class<?>[] {Object.class, Integer.TYPE, Integer.TYPE}, parmIdx, obj, tsqlt, sol);
            wrapped.setObject(parmIdx, obj, tsqlt, sol);
        });
    }

    @Override
    public void setRef(int parmIdx, Ref ref) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setRef", new Class<?>[] {Ref.class}, parmIdx, ref);
            wrapped.setRef(parmIdx, ref);
        });
    }

    @Override
    public void setRowId(int parmIdx, RowId rowId) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setRowId", new Class<?>[] {RowId.class}, parmIdx, rowId);
            wrapped.setRowId(parmIdx, rowId);
        });
    }

    @Override
    public void setShort(int parmIdx, short sht) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setShort", new Class<?>[] {Short.TYPE}, parmIdx, sht);
            wrapped.setShort(parmIdx, sht);
        });
    }

    @Override
    public void setSQLXML(int parmIdx, SQLXML sx) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setSQLXML", new Class<?>[] {SQLXML.class}, parmIdx, sx);
            wrapped.setSQLXML(parmIdx, sx);
        });
    }

    @Override
    public void setString(int parmIdx, String string) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setString", new Class<?>[] {String.class}, parmIdx, string);
            wrapped.setString(parmIdx, string);
        });
    }

    @Override
    public void setTime(int parmIdx, Time time) throws UcanaccessSQLException {
        tryCatch(() -> {
            Calendar cl = Calendar.getInstance();
            cl.setTime(time);
            cl.set(1899, 11, 30);
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, parmIdx, ts);
            wrapped.setTimestamp(parmIdx, ts);
        });
    }

    @Override
    public void setTime(int parmIdx, Time time, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            Calendar cl = Calendar.getInstance();
            cal.setTime(time);
            cl.set(1899, 11, 30, cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
            cl.set(Calendar.MILLISECOND, 0);
            Timestamp ts = new Timestamp(cl.getTimeInMillis());
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, parmIdx, ts);
            wrapped.setTimestamp(parmIdx, ts);
        });
    }

    @Override
    public void setTimestamp(int parmIdx, Timestamp ts) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class}, parmIdx, ts);
            wrapped.setTimestamp(parmIdx, ts);
        });
    }

    @Override
    public void setTimestamp(int parmIdx, Timestamp ts, Calendar cal) throws UcanaccessSQLException {
        tryCatch(() -> {
            addMementoEntry("setTimestamp", new Class<?>[] {Timestamp.class, Calendar.class}, parmIdx, ts, cal);
            wrapped.setTimestamp(parmIdx, ts, cal);
        });
    }

    /**
     * @deprecated Use {@code setCharacterStream}
     */
    @Override
    @Deprecated
    public void setUnicodeStream(int parmIdx, InputStream is, int length) throws UcanaccessSQLException {
        try {
            is = markableInputStream(is, length);
            addMementoEntry("setUnicodeStream", new Class<?>[] {InputStream.class, Integer.TYPE}, parmIdx, is, length);
            wrapped.setUnicodeStream(parmIdx, is, length);
            resetInputStream(is);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void setURL(int parmIdx, URL url) throws UcanaccessSQLException {
        tryCatch(() -> {
            String arg = "#" + url.toString() + "#";
            addMementoEntry("setString", new Class<?>[] {String.class}, parmIdx, arg);
            wrapped.setString(parmIdx, arg);
        });
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws UcanaccessSQLException {
        return tryCatch(() -> wrapped.unwrap(iface));
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
