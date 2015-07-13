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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import net.ucanaccess.util.Logger;


public class UcanaccessDataSource implements Serializable, Referenceable,
		DataSource {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8574198937631043152L;
	private String accessPath ;
	private int loginTimeout = 0;
	private transient PrintWriter logWriter = Logger.getLogPrintWriter();
	private String password = "";
	private String user ;
	
	public UcanaccessDataSource() {
	}
	
	public String getAccessPath() {
		return accessPath;
	}
	
	public Connection getConnection() throws SQLException {
		return getConnection(user, password);
	}
	
	public Connection getConnection(String username, String password)
			throws SQLException {
		Properties props = new Properties();
		if (username != null) {
			props.put("user", username);
		}
		if (password != null) {
			props.put("password", password);
		}
		return new UcanaccessDriver().connect(UcanaccessDriver.URL_PREFIX+accessPath, props);
	}
	
	public int getLoginTimeout() throws SQLException {
		return this.loginTimeout;
	}
	
	public java.io.PrintWriter getLogWriter() throws SQLException {
		return logWriter;
	}
	
	public java.util.logging.Logger getParentLogger()
			throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}
	
	public Reference getReference() throws NamingException {
		String clazz = UcanaccessDataSourceFactory.class.getName();
		Reference ref = new Reference(this.getClass().getName(), clazz, null);
		ref.add(new StringRefAddr("accessPath", this.getAccessPath()));
		ref.add(new StringRefAddr("user", getUser()));
		ref.add(new StringRefAddr("password", password));
		return ref;
	}
		
	public String getUser() {
		return user;
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	public void setAccessPath(String accessPath) {
		this.accessPath = accessPath;
	}
	
	public void setLoginTimeout(int seconds) throws SQLException {
		loginTimeout = seconds;
	}
	
	public void setLogWriter(PrintWriter logWriter) throws SQLException {
		Logger.setLogPrintWriter(logWriter);
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setUser(String user) {
		this.user = user;
	}

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}
