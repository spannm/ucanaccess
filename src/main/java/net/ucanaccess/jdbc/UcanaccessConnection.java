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

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import  org.hsqldb.jdbc.JDBCConnection;

import net.ucanaccess.commands.CompositeCommand;
import net.ucanaccess.commands.IFeedbackAction;
import net.ucanaccess.commands.ICommand;
import net.ucanaccess.commands.ICursorCommand;
import net.ucanaccess.commands.ICommand.TYPES;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Database;

public class UcanaccessConnection implements Connection {
	private static ThreadLocal<Context> ctx = new ThreadLocal<Context>();
	private boolean feedbackState;
	private LinkedList<ICommand> commands = new LinkedList<ICommand>();
	private Connection hsqlDBConnection;
	private HashMap<Savepoint, String> savepointsMap = new HashMap<Savepoint, String>();
	private DBReference ref;
	private boolean checkModified = false;
	private boolean autoCommit = true;
	private Properties clientInfo;
	// test only!!!!
	private boolean testRollback;
	private Session session;
	private SQLWarning warnings;
	private String url;
	private  UcanaccessStatement currentStatement;
	private Object lastGeneratedKey;
	private String refId;
	
	final static String BATCH_ID="BATCH_ID";


	public synchronized static UcanaccessConnection getCtxConnection() {
		if(ctx==null)return null;
		return ctx.get().getCurrentConnection();
	}
	
	public synchronized static boolean hasContext() {
		return ctx.get()!=null;
	}

	public synchronized static String getCtxExcId() {
		return ctx.get().getCurrentExecId();
	}

	public synchronized static void setCtxConnection(UcanaccessConnection conn) {
		ctx.set(new Context(conn));
	}

	public synchronized static void setCtxExecId(String id) {
		ctx.get().setCurrentExecId(id);
	}

	public UcanaccessConnection(DBReference ref, Properties clientInfo,
			Session session) throws UcanaccessSQLException {
		try {
			this.ref = ref;
			this.refId=ref.getId();
			ref.incrementActiveConnection();
			this.session = session;
			this.hsqlDBConnection = ref.getHSQLDBConnection(session);
			this.clientInfo = clientInfo;
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	

	Object getLastGeneratedKey() {
		return lastGeneratedKey;
	}
	
	
	String preprocess(String sql){
		if (SQLConverter.hasIdentity(sql)){
			return SQLConverter.preprocess(sql, lastGeneratedKey);
		}
		return sql;
	}
	
	void setCurrentStatement(UcanaccessStatement currentStatement){
		this.currentStatement=currentStatement;
	}
	
	public void setGeneratedKey(Object key){
		this.lastGeneratedKey=key;
		if(currentStatement!=null)
		currentStatement.setGeneratedKey( key);
	}
	
	public boolean isTestRollback() {
		return testRollback;
	}
	
	// test only!!!!
	@SuppressWarnings("unused")
	private void setTestRollback(boolean testRollback) {
		this.testRollback = testRollback;
	}

	public void addFunctions(Class<?> clazz) throws SQLException {
		LoadJet lfa = new LoadJet(this.ref.getHSQLDBConnection(session),
				ref.getDbIO());
		lfa.addFunctions(clazz);
	}

	public void reloadDbIO() throws IOException {
		synchronized (UcanaccessConnection.class) {
			this.ref.reloadDbIO();
		}
	}

	public synchronized boolean add(ICommand c4io) {
		if (c4io.getType().equals(TYPES.UPDATE)
				|| c4io.getType().equals(TYPES.DELETE)) {
			ICommand last = commands.size() > 0 ? commands.getLast() : null;
			ICursorCommand c4ioc = (ICursorCommand) c4io;
			if (last != null&&! last.getExecId().equals(BATCH_ID) && last.getExecId().equals(c4io.getExecId())
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

	public void clearWarnings() throws SQLException {
		this.warnings = null;
	}

	public void close() throws SQLException {
		try {
			hsqlDBConnection.close();
			ref.decrementActiveConnection(this.session);

		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void commit() throws SQLException {
		synchronized (this.getClass()) {

			try {
				if (this.isReadOnly() && this.commands.size() > 0) {
					this.rollback();
					if(this.ref.isReadOnlyFileFormat()){
						throw new UcanaccessSQLException(
								ExceptionMessages.ACCESS_97);
					}
					throw new UcanaccessSQLException(
							ExceptionMessages.CONCURRENT_PROCESS_ACCESS);
				}

				this.flushIO();
				hsqlDBConnection.commit();
				if (commands.size() > 0) {
					this.ref.updateLastModified();
				}

			} catch (SQLException e) {
				throw new UcanaccessSQLException(e);
			} finally {
				finalizeEnlistedResources();
				this.checkModified=true;
			}
		}
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		try {
			checkConnection();
			return hsqlDBConnection.createArrayOf(typeName, elements);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Blob createBlob() throws SQLException {
		try {
			checkConnection();
			return new UcanaccessBlob(hsqlDBConnection.createBlob());
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	private void checkConnection() throws UcanaccessSQLException {
		if(this.autoCommit||this.isCheckModified()){
			this.checkLastModified();
		}
		
	}

	public Clob createClob() throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public NClob createNClob() throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public SQLXML createSQLXML() throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public Statement createStatement() throws SQLException {
		try {
			checkConnection();
			return new UcanaccessStatement(hsqlDBConnection.createStatement(),
					this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		try {
			checkConnection();
			return new UcanaccessStatement(hsqlDBConnection.createStatement(
					resultSetType, resultSetConcurrency), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		try {
			checkConnection();
			return new UcanaccessStatement(hsqlDBConnection.createStatement(
					resultSetType, resultSetConcurrency, resultSetHoldability),
					this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	
 

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		try {
			checkConnection();
			return hsqlDBConnection.createStruct(typeName, attributes);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	private void flushIO() throws SQLException {
		ArrayList<IFeedbackAction> ibal = new ArrayList<IFeedbackAction>();
		LinkedList<ICommand> executed = new LinkedList<ICommand>();

		try {

			for (ICommand command : commands) {
				for (IFeedbackAction ib : ibal) {
					ib.doAction(command);
				}
				IFeedbackAction ib = command.persist();
				executed.add(command);
				if (ib != null) {
					ibal.add(ib);
				}

			}
			if (testRollback)
				throw new Error("Test");
		} catch (Throwable t) {
			t.printStackTrace();
			this.hsqlDBConnection.rollback();
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
			this.ref.updateLastModified();
			try {
				this.ref.getDbIO().flush();
				this.unloadDB();
			}
			catch (IOException e) {}
			throw new UcanaccessSQLException(t);
		}
		try {
			this.ref.getDbIO().flush();

		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}

	}

	private void finalizeEnlistedResources() {
		commands.clear();
		this.savepointsMap.clear();
		setCtxConnection(null);
		setCtxExecId(null);
	}

	public boolean getAutoCommit() throws SQLException {
		return this.autoCommit;
	}

	public String getCatalog() throws SQLException {
		try {
			return hsqlDBConnection.getCatalog();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Properties getClientInfo() throws SQLException {
		return this.clientInfo;
	}

	public String getClientInfo(String name) throws SQLException {
		return this.clientInfo.getProperty(name);
	}

	public Database getDbIO() {
		return this.ref.getDbIO();
	}

	public int getHoldability() throws SQLException {
		try {
			return hsqlDBConnection.getHoldability();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Connection getHSQLDBConnection() {
		return hsqlDBConnection;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		try {
			return new UcanaccessDatabaseMetadata(
					hsqlDBConnection.getMetaData(), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getTransactionIsolation() throws SQLException {
		try {
			return hsqlDBConnection.getTransactionIsolation();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		try {
			return hsqlDBConnection.getTypeMap();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public SQLWarning getWarnings() throws SQLException {
		return this.warnings;
	}

	public void setWarnings(SQLWarning warnings) {
		this.warnings = warnings;
	}

	public void addWarnings(SQLWarning warnings) {
		if (this.warnings == null)
			setWarnings(warnings);
		else
			this.warnings.setNextWarning(warnings);
	}

	public boolean isFeedbackState() {
		return feedbackState;
	}

	public boolean isClosed() throws SQLException {
		try {
			return hsqlDBConnection.isClosed();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean isReadOnly() throws SQLException {
		return ref.isReadOnly();
	}

	public boolean isValid(int timeout) throws SQLException {
		try {
			return hsqlDBConnection.isValid(timeout);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		try {
			return hsqlDBConnection.isWrapperFor(arg0);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public String nativeSQL(String sql) throws SQLException {
		return SQLConverter.convertSQL(sql).getSql();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new FeatureNotSupportedException();
	}

	private NormalizedSQL prepare(String sql) throws SQLException {
		checkConnection();
		return SQLConverter.convertSQL(sql,this);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		try {
			if(SQLConverter.checkDDL(sql)){
			    Logger.log(Logger.Messages.STATEMENT_DDL);
			    return new UcanaccessPreparedStatement(prepare(sql),
						null, this);
		     }
		
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement(nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql())), this);
		} catch (SQLException e) {
			
			throw new UcanaccessSQLException(e);
		}
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		try {
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement( nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), autoGeneratedKeys),
					this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		String oldSql=sql;
		try {
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement(nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), resultSetType,
							resultSetConcurrency), this);
		}
		
		catch (SQLException e) {
		    if(resultSetType==ResultSet.TYPE_SCROLL_SENSITIVE&&resultSetConcurrency==ResultSet.CONCUR_UPDATABLE){
		    	return  prepareStatement(oldSql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY); 
		    }
			throw new UcanaccessSQLException(e);
		}
	}
	

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		try {
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement(nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), resultSetType,
							resultSetConcurrency, resultSetHoldability), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		try {
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement(nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), columnIndexes), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		try { 
			NormalizedSQL nsql= prepare(sql);
			return new UcanaccessPreparedStatement(nsql,
					hsqlDBConnection.prepareStatement(preprocess(nsql.getSql()), columnNames), this);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		try {
			hsqlDBConnection.releaseSavepoint(((UcanaccessSavepoint)savepoint).getWrapped());
			this.savepointsMap.remove(savepoint);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void rollback() throws SQLException {
		try {
			
			hsqlDBConnection.rollback();
			
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		} finally {
			finalizeEnlistedResources();
			this.checkModified=true;
		}
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		try {
			
			hsqlDBConnection.rollback(((UcanaccessSavepoint)savepoint).getWrapped());
			String lastId = this.savepointsMap.get(savepoint);
			boolean remove = false;
			Iterator<ICommand> it = commands.iterator();
			while (it.hasNext()) {
				ICommand c4io = it.next();
				if (remove && !c4io.getExecId().equals(lastId))
					it.remove();
				remove = remove || c4io.getExecId().equals(lastId);
			}
			this.checkModified=true;
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (!autoCommit) {
			this.checkLastModified();
		}else{
			this.checkModified=false;
		}
		this.autoCommit = autoCommit;
	}

	public void setFeedbackState(boolean feedbackState) {
		this.feedbackState = feedbackState;
	}

	public void setCatalog(String catalog) throws SQLException {
		throw new FeatureNotSupportedException();
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		hsqlDBConnection.setClientInfo(properties);
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		hsqlDBConnection.setClientInfo(name, value);
	}

	public void setHoldability(int holdability) throws SQLException {
		try {
			hsqlDBConnection.setHoldability(holdability);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		try {
			hsqlDBConnection.setReadOnly(readOnly);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Savepoint setSavepoint() throws SQLException {
		try {
			Savepoint sp = new UcanaccessSavepoint(hsqlDBConnection.setSavepoint());
			if (this.commands.size() > 0) {
				// last to commit
				this.savepointsMap.put(sp, this.commands.getLast().getExecId());
			}
			return sp;
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		try {
			Savepoint sp = new UcanaccessSavepoint(
					hsqlDBConnection.setSavepoint(name));
			
			if (this.commands.size() > 0) {
				// last to commit
				this.savepointsMap.put(sp, this.commands.getLast().getExecId());
			}
			return sp;
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void setTransactionIsolation(int level) throws SQLException {
		try {
			hsqlDBConnection.setTransactionIsolation(level);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		try {
			hsqlDBConnection.setTypeMap(map);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public <T> T unwrap(Class<T> arg0) throws SQLException {
		try {
			return hsqlDBConnection.unwrap(arg0);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	@Override
	public String toString() {
		String w = super.toString();
		return w + "[" + this.ref.getDbFile() + "]";
	}
	
	

	void  checkLastModified() throws UcanaccessSQLException {
		try {
				
				this.checkModified=false;
				if(!this.refId.equals(ref.getId())){
					this.hsqlDBConnection=ref.getHSQLDBConnection(session);
				}
			    synchronized (UcanaccessDriver.class) {
				    this.hsqlDBConnection = this.ref.checkLastModified(
					this.hsqlDBConnection, session);
			    }
				this.refId=ref.getId();
			
		} catch (Exception e) {
			throw new UcanaccessSQLException(e);
		}

	}
	
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean isShowSchema() {
		return this.ref.isShowSchema();
	}

	
	public void setSchema(String schema) throws SQLException {
		
	}

	
	public String getSchema() throws SQLException {
		return "";
	}

	public void unloadDB() throws UcanaccessSQLException{
		try {
			synchronized (UcanaccessDriver.class) {
				this.ref.shutdown(session);
			}
			
		} catch (Exception e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void abort(Executor executor) throws SQLException {
		try {
			((JDBCConnection)hsqlDBConnection).abort(executor);
		} catch (Exception e) {
			throw new UcanaccessSQLException(e);
		}
	}

	
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
	}

	
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}


	public boolean isCheckModified() {
		return checkModified;
	}


	

}
