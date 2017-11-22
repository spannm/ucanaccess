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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;

import org.hsqldb.jdbc.JDBCPreparedStatement;
import org.hsqldb.jdbc.JDBCStatement;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

public class UcanaccessStatement implements Statement {
    private UcanaccessConnection connection;
    //CHECKSTYLE:OFF
    protected Statement          wrapped;
    //CHECKSTYLE:ON
    private Object               generatedKey;
    private Map<String, String>  aliases;
    private boolean              enableDisable;

    protected Map<String, String> getAliases() {
        return aliases;
    }

    protected void setAliases(Map<String, String> _aliases) {
        this.aliases = _aliases;
    }

    public UcanaccessStatement(Statement _wrapped, UcanaccessConnection _conn) throws SQLException {
        this.wrapped = _wrapped;
        this.connection = _conn;
    }

    private String convertSQL(String sql, UcanaccessConnection conn) {
        if (SQLConverter.checkDDL(sql)) {
            return sql;
        }
        NormalizedSQL nsql = SQLConverter.convertSQL(sql, conn);
        this.aliases = nsql.getAliases();
        return preprocess(nsql.getSql());
    }

    private String convertSQL(String sql) {
        if (SQLConverter.checkDDL(sql)) {
            return sql;
        }
        NormalizedSQL nsql = SQLConverter.convertSQL(sql);
        this.aliases = nsql.getAliases();
        return preprocess(nsql.getSql());
    }

    private String preprocess(String sql) {
        return this.connection.preprocess(sql);
    }

    @Override
    public void addBatch(String batch) throws SQLException {
        try {

            wrapped.addBatch(SQLConverter.convertSQL(batch).getSql());

        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void cancel() throws SQLException {
        try {
            wrapped.cancel();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        try {
            wrapped.clearBatch();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        try {
            wrapped.clearWarnings();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            wrapped.close();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public void closeOnCompletion() throws SQLException {
        try {
            if (wrapped instanceof JDBCStatement) {
                ((JDBCStatement) wrapped).closeOnCompletion();
            } else if (wrapped instanceof JDBCPreparedStatement) {
                ((JDBCPreparedStatement) wrapped).closeOnCompletion();
            } else if (wrapped instanceof UcanaccessStatement) {
                UcanaccessStatement stat = (UcanaccessStatement) wrapped;
                stat.closeOnCompletion();
            } else {
                throw new UcanaccessSQLException(ExceptionMessages.CLOSE_ON_COMPLETION_STATEMENT);
            }
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    protected void checkLastModified() throws SQLException {

        if (connection.getAutoCommit() || connection.isCheckModified()) {
            Connection hsqldb = this.wrapped.getConnection();
            connection.checkLastModified();
            if (hsqldb != connection.getHSQLDBConnection()) {
                reset();

            }
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql, this.connection);
            return new Execute(this, sql).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql, this.connection);
            return new Execute(this, sql, autoGeneratedKeys).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, int[] indexes) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql, this.connection);
            return new Execute(this, sql, indexes).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql, this.connection);
            return new Execute(this, sql, columnNames).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            return new ExecuteUpdate(this).executeBatch();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql, this.connection);

            return new UcanaccessResultSet(wrapped.executeQuery(sql), this);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql);
            return new ExecuteUpdate(this, sql).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int arg) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql);
            return new ExecuteUpdate(this, sql, arg).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int[] arg) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql);
            return new ExecuteUpdate(this, sql, arg).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, String[] arg) throws SQLException {
        try {
            this.connection.setCurrentStatement(this);
            checkLastModified();
            sql = convertSQL(sql);
            return new ExecuteUpdate(this, sql, arg).execute();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        try {
            return wrapped.getFetchDirection();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        try {
            return wrapped.getFetchSize();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            checkLastModified();
            Connection conn = this.connection.getHSQLDBConnection();
            Statement st = conn.createStatement();
            StringBuffer sql = new StringBuffer();

            if (this.generatedKey != null) {
                sql.append(" SELECT ")

                        .append(this.generatedKey instanceof String ? "'" + this.generatedKey + "'" : this.generatedKey)
                        .append(" AS GENERATED_KEY ").append(" FROM DUAL");

            } else {
                sql.append(" SELECT ").append(0).append(" AS GENERATED_KEY ").append(" FROM DUAL where 1=2 ");
            }

            return new UcanaccessResultSet(st.executeQuery(sql.toString()), this);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        try {
            return wrapped.getMaxFieldSize();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        try {
            return wrapped.getMaxRows();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        try {
            return wrapped.getMoreResults();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        try {
            return wrapped.getMoreResults(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        try {
            return wrapped.getQueryTimeout();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            ResultSet rs = wrapped.getResultSet();
            if (wrapped == null || rs == null) {
                return null;
            }
            return new UcanaccessResultSet(rs, this);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        try {
            return wrapped.getResultSetConcurrency();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            return wrapped.getResultSetHoldability();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getResultSetType() throws SQLException {
        try {
            return wrapped.getResultSetType();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        try {

            int i = wrapped.getUpdateCount();
            if (i == -1 && this.enableDisable) {
                return 0;
            }
            return i;
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            return wrapped.getWarnings();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    Statement getWrapped() {
        return wrapped;
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            return wrapped.isClosed();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public boolean isCloseOnCompletion() throws SQLException {
        try {
            return ((JDBCStatement) wrapped).isCloseOnCompletion();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        try {
            return wrapped.isPoolable();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        try {
            return wrapped.isWrapperFor(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        try {
            wrapped.setCursorName(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        try {
            wrapped.setEscapeProcessing(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        try {
            wrapped.setFetchDirection(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setFetchSize(int arg0) throws SQLException {
        try {
            wrapped.setFetchSize(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        try {
            wrapped.setMaxFieldSize(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setMaxRows(int arg0) throws SQLException {
        try {
            wrapped.setMaxRows(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        try {
            wrapped.setPoolable(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        try {
            wrapped.setQueryTimeout(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        try {
            return wrapped.unwrap(arg0);
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    protected void reset() throws SQLException {
        Statement old = this.wrapped;
        reset(((UcanaccessConnection) this.getConnection()).getHSQLDBConnection().createStatement(
                wrapped.getResultSetType(), wrapped.getResultSetConcurrency(), wrapped.getResultSetHoldability()));
        old.close();
    }

    protected void reset(Statement st) throws SQLException {
        int maxr = this.wrapped.getMaxRows();
        int maxf = this.wrapped.getMaxFieldSize();
        int direction = this.wrapped.getFetchDirection();
        int fs = this.wrapped.getFetchSize();
        int qt = this.wrapped.getQueryTimeout();
        this.wrapped = st;
        this.wrapped.setMaxRows(maxr);
        this.wrapped.setMaxFieldSize(maxf);
        this.wrapped.setFetchDirection(direction);
        this.wrapped.setFetchSize(fs);
        this.wrapped.setQueryTimeout(qt);
    }

    public void setGeneratedKey(Object key) {
        this.generatedKey = key;
    }

    boolean isEnableDisable() {
        return enableDisable;
    }

    void setEnableDisable(boolean _enableDisable) {
        this.enableDisable = _enableDisable;
    }

}
