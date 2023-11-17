package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database;
import net.ucanaccess.commands.CompositeCommand;
import net.ucanaccess.commands.ICommand;
import net.ucanaccess.commands.ICommand.TYPES;
import net.ucanaccess.commands.ICursorCommand;
import net.ucanaccess.commands.IFeedbackAction;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.log.Logger;
import net.ucanaccess.log.LoggerMessageEnum;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class UcanaccessConnection implements Connection {
    private static ThreadLocal<Context> ctx           = new ThreadLocal<>();
    private boolean                     feedbackState;
    private LinkedList<ICommand>        commands      = new LinkedList<>();
    private Connection                  hsqlDBConnection;
    private Map<Savepoint, String>      savepointsMap = new HashMap<>();
    private DBReference                 ref;
    private boolean                     checkModified = false;
    private boolean                     autoCommit    = true;
    private Properties                  clientInfo;
    private Session                     session;
    private SQLWarning                  warnings;
    private String                      url;
    private UcanaccessStatement         currentStatement;
    private Object                      lastGeneratedKey;
    private String                      refId;

    static final String                 BATCH_ID      = "BATCH_ID";

    public static synchronized UcanaccessConnection getCtxConnection() {
        if (ctx == null) {
            return null;
        }
        return ctx.get().getCurrentConnection();
    }

    public static synchronized boolean hasContext() {
        return ctx.get() != null;
    }

    public static synchronized String getCtxExcId() {
        return ctx.get().getCurrentExecId();
    }

    public static synchronized void setCtxConnection(UcanaccessConnection conn) {
        ctx.set(new Context(conn));
    }

    public static synchronized void setCtxExecId(String id) {
        ctx.get().setCurrentExecId(id);
    }

    public UcanaccessConnection(DBReference _ref, Properties _clientInfo, Session _session)
            throws UcanaccessSQLException {
        try {
            ref = _ref;
            refId = _ref.getId();
            _ref.incrementActiveConnection();
            session = _session;
            hsqlDBConnection = _ref.getHSQLDBConnection(_session);
            clientInfo = _clientInfo;
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    Object getLastGeneratedKey() {
        return lastGeneratedKey;
    }

    String preprocess(String sql) {
        if (SQLConverter.hasIdentity(sql)) {
            return SQLConverter.preprocess(sql, lastGeneratedKey);
        }
        return sql;
    }

    void setCurrentStatement(UcanaccessStatement _currentStatement) {
        currentStatement = _currentStatement;
    }

    public void setGeneratedKey(Object key) {
        lastGeneratedKey = key;
        if (currentStatement != null) {
            currentStatement.setGeneratedKey(key);
        }
    }

    public void addFunctions(Class<?> clazz) throws SQLException {
        LoadJet lfa = new LoadJet(ref.getHSQLDBConnection(session), ref.getDbIO());
        lfa.addFunctions(clazz);
    }

    public void reloadDbIO() throws IOException {
        synchronized (UcanaccessConnection.class) {
            ref.reloadDbIO();
        }
    }

    public synchronized boolean add(ICommand c4io) {
        if (c4io.getType().equals(TYPES.UPDATE) || c4io.getType().equals(TYPES.DELETE)) {
            ICommand last = !commands.isEmpty() ? commands.getLast() : null;
            ICursorCommand c4ioc = (ICursorCommand) c4io;
            if (last != null && !last.getExecId().equals(BATCH_ID) && last.getExecId().equals(c4io.getExecId())
                    && last.getTableName().equals(c4io.getTableName())) {
                return ((CompositeCommand) last).add(c4ioc);
            } else {
                CompositeCommand cc4io = new CompositeCommand();
                cc4io.add(c4ioc);
                c4io = cc4io;
            }
        }
        return commands.add(c4io);
    }

    @Override
    public void clearWarnings() {
        warnings = null;
    }

    @Override
    public void close() throws SQLException {
        try {
            hsqlDBConnection.close();
            ref.decrementActiveConnection(session);

        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void commit() throws SQLException {
        synchronized (UcanaccessConnection.class) {

            try {
                if (isReadOnly() && !commands.isEmpty()) {
                    rollback();
                    if (ref.isReadOnlyFileFormat()) {
                        throw new UcanaccessSQLException(ExceptionMessages.ACCESS_97);
                    }
                    throw new UcanaccessSQLException(ExceptionMessages.CONCURRENT_PROCESS_ACCESS);
                }

                flushIO();
                hsqlDBConnection.commit();
                if (!commands.isEmpty()) {
                    ref.updateLastModified();
                }

            } catch (SQLException _ex) {
                throw new UcanaccessSQLException(_ex);
            } finally {
                finalizeEnlistedResources();
                checkModified = true;
            }
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            checkConnection();
            return hsqlDBConnection.createArrayOf(typeName, elements);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        try {
            checkConnection();
            return new UcanaccessBlob(UcanaccessBlob.createBlob(this), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    public Blob createBlob(File fl) throws SQLException {
        try {
            checkConnection();
            return new UcanaccessBlob(UcanaccessBlob.createBlob(fl, this), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    private void checkConnection() throws UcanaccessSQLException {
        if (autoCommit || isCheckModified()) {
            checkLastModified();
        }

    }

    @Override
    public Clob createClob() throws SQLException {
        throw UcanaccessRuntimeException.featureNotSupported();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw UcanaccessRuntimeException.featureNotSupported();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw UcanaccessRuntimeException.featureNotSupported();
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkConnection();
        return new UcanaccessStatement(hsqlDBConnection.createStatement(), this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkConnection();
        return new UcanaccessStatement(hsqlDBConnection.createStatement(resultSetType, resultSetConcurrency), this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkConnection();
        return new UcanaccessStatement(
                hsqlDBConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        try {
            checkConnection();
            return hsqlDBConnection.createStruct(typeName, attributes);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    private void flushIO() throws SQLException {
        List<IFeedbackAction> ibal = new ArrayList<>();
        LinkedList<ICommand> executed = new LinkedList<>();

        try {

            for (ICommand command : commands) {
                for (IFeedbackAction ib : ibal) {
                    ib.doAction(command);
                }
                IFeedbackAction ib = command.persist();
                executed.add(command);
                if (ib != null) {
                    if (command instanceof CompositeCommand) {
                        ib.doAction(command);
                    } else {
                        ibal.add(ib);
                    }
                }

            }

            afterFlushIoHook();

        } catch (Throwable _t) {
            _t.printStackTrace();
            hsqlDBConnection.rollback();
            ibal.clear();
            Iterator<ICommand> it = executed.descendingIterator();
            while (it.hasNext()) {
                ICommand command = it.next();
                for (IFeedbackAction ib : ibal) {
                    ib.doAction(command);
                }
                IFeedbackAction ib = command.rollback();

                if (ib != null) {
                    ibal.add(ib);
                }
            }
            ref.updateLastModified();
            try {
                ref.getDbIO().flush();
                unloadDB();
            } catch (IOException _ex) {
                _ex.printStackTrace();
            }
            if (UcanaccessSQLException.class.isInstance(_t)) {
                throw UcanaccessSQLException.class.cast(_t);
            }
            throw new UcanaccessSQLException(_t);
        }

        try {
            ref.getDbIO().flush();
        } catch (IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }

    }

    /**
     * Extension hook called towards end of {@link #flushIO()}.
     */
    void afterFlushIoHook() {
    }

    private void finalizeEnlistedResources() {
        commands.clear();
        savepointsMap.clear();
        setCtxConnection(null);
        setCtxExecId(null);
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public String getCatalog() throws SQLException {
        try {
            return hsqlDBConnection.getCatalog();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Properties getClientInfo() {
        return clientInfo;
    }

    @Override
    public String getClientInfo(String _name) {
        return clientInfo.getProperty(_name);
    }

    public Database getDbIO() {
        return ref.getDbIO();
    }

    @Override
    public int getHoldability() throws SQLException {
        try {
            return hsqlDBConnection.getHoldability();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    public Connection getHSQLDBConnection() {
        return hsqlDBConnection;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            return new UcanaccessDatabaseMetadata(hsqlDBConnection.getMetaData(), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            return hsqlDBConnection.getTransactionIsolation();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            return hsqlDBConnection.getTypeMap();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public SQLWarning getWarnings() {
        return warnings;
    }

    public void setWarnings(SQLWarning _warnings) {
        warnings = _warnings;
    }

    public void addWarnings(SQLWarning _warnings) {
        if (warnings == null) {
            setWarnings(_warnings);
        } else {
            warnings.setNextWarning(_warnings);
        }
    }

    public boolean isFeedbackState() {
        return feedbackState;
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            return hsqlDBConnection.isClosed();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ref.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            return hsqlDBConnection.isValid(timeout);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        try {
            return hsqlDBConnection.isWrapperFor(arg0);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String nativeSQL(String sql) {
        return SQLConverter.convertSQL(sql).getSql();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessCallableStatement(nsql, hsqlDBConnection.prepareCall(sql), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessCallableStatement(nsql,
                    hsqlDBConnection.prepareCall(sql, resultSetType, resultSetConcurrency), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        try {
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessCallableStatement(nsql,
                    hsqlDBConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    private NormalizedSQL prepare(String sql) throws SQLException {
        checkConnection();
        return SQLConverter.convertSQL(sql, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }

            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql, hsqlDBConnection.prepareStatement(preprocess(nsql.getSql())),
                    this);
        } catch (SQLException _ex) {

            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql,
                    hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), autoGeneratedKeys), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql,
                    hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), resultSetType, resultSetConcurrency),
                    this);
        } catch (SQLException _ex) {
            if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE
                    && resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
                return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            }
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }

            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql, hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()),
                    resultSetType, resultSetConcurrency, resultSetHoldability), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql,
                    hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), columnIndexes), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            if (SQLConverter.checkDDL(sql)) {
                Logger.log(LoggerMessageEnum.STATEMENT_DDL);
                return new UcanaccessPreparedStatement(sql, this);
            }
            NormalizedSQL nsql = prepare(sql);
            return new UcanaccessPreparedStatement(nsql,
                    hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), columnNames), this);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            hsqlDBConnection.releaseSavepoint(((UcanaccessSavepoint) savepoint).getWrapped());
            savepointsMap.remove(savepoint);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void rollback() throws SQLException {
        try {

            hsqlDBConnection.rollback();

        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        } finally {
            finalizeEnlistedResources();
            checkModified = true;
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        try {

            hsqlDBConnection.rollback(((UcanaccessSavepoint) savepoint).getWrapped());
            String lastId = savepointsMap.get(savepoint);
            boolean remove = false;
            Iterator<ICommand> it = commands.iterator();
            while (it.hasNext()) {
                ICommand c4io = it.next();
                if (remove && !c4io.getExecId().equals(lastId)) {
                    it.remove();
                }
                remove = remove || c4io.getExecId().equals(lastId);
            }
            checkModified = true;
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setAutoCommit(boolean _autoCommit) throws SQLException {
        if (!_autoCommit) {
            checkLastModified();
        } else {
            checkModified = false;
        }
        autoCommit = _autoCommit;
    }

    public void setFeedbackState(boolean _feedbackState) {
        feedbackState = _feedbackState;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw UcanaccessRuntimeException.featureNotSupported();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        hsqlDBConnection.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        hsqlDBConnection.setClientInfo(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        try {
            hsqlDBConnection.setHoldability(holdability);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            hsqlDBConnection.setReadOnly(readOnly);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            Savepoint sp = new UcanaccessSavepoint(hsqlDBConnection.setSavepoint());
            if (!commands.isEmpty()) {
                // last to commit
                savepointsMap.put(sp, commands.getLast().getExecId());
            }
            return sp;
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            Savepoint sp = new UcanaccessSavepoint(hsqlDBConnection.setSavepoint(name));

            if (!commands.isEmpty()) {
                // last to commit
                savepointsMap.put(sp, commands.getLast().getExecId());
            }
            return sp;
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            hsqlDBConnection.setTransactionIsolation(level);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        try {
            hsqlDBConnection.setTypeMap(map);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        try {
            return hsqlDBConnection.unwrap(arg0);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "[" + ref.getDbFile() + "]";
    }

    void checkLastModified() throws UcanaccessSQLException {
        try {

            checkModified = false;
            if (!refId.equals(ref.getId())) {
                hsqlDBConnection = ref.getHSQLDBConnection(session);
            }
            synchronized (UcanaccessDriver.class) {
                hsqlDBConnection = ref.checkLastModified(hsqlDBConnection, session);
            }
            refId = ref.getId();

        } catch (Exception _ex) {
            throw new UcanaccessSQLException(_ex);
        }

    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String _url) {
        url = _url;
    }

    public boolean isShowSchema() {
        return ref.isShowSchema();
    }

    @Override
    public void setSchema(String schema) {

    }

    @Override
    public String getSchema() {
        return "";
    }

    public void unloadDB() throws UcanaccessSQLException {
        try {
            synchronized (UcanaccessDriver.class) {
                ref.shutdown(session);
            }

        } catch (Exception _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        try {
            hsqlDBConnection.abort(executor);
        } catch (Exception _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    public boolean isCheckModified() {
        return checkModified;
    }

}
