/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

 */
package net.ucanaccess.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;


import com.healthmarketscience.jackcess.Database.FileFormat;

import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.util.Logger;
import net.ucanaccess.util.Logger.Messages;

public final class UcanaccessDriver implements Driver {
	public static final String URL_PREFIX = "jdbc:ucanaccess://";
	static {
		try {
			DriverManager.registerDriver(new UcanaccessDriver());
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
			

		} catch (ClassNotFoundException e) {
			Logger.logMessage(Messages.HSQLDB_DRIVER_NOT_FOUND);
			throw new RuntimeException(e.getMessage());
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		return (url.startsWith(URL_PREFIX) && url.length() > URL_PREFIX
				.length());
	}

	public Connection connect(String url, Properties pr) throws SQLException {
		// ok wimp
		if (!this.acceptsURL(url)) {
			return null;
		}
		readProperties(pr, url);
		String fileMdbPath = url.indexOf(";") > 0 ? url.substring(URL_PREFIX
				.length(), url.indexOf(";")) : url.substring(URL_PREFIX
				.length());
		File mdb = new File(fileMdbPath);
		DBReferenceSingleton as = DBReferenceSingleton.getInstance();

		synchronized (UcanaccessDriver.class) {
			try {
				Session session = new Session();

				boolean alreadyLoaded = as.loaded(mdb);
				FileFormat ff = null;
				if (pr.containsKey("newdatabaseversion")) {
					if (!mdb.exists()) {
						
						ff = FileFormat.valueOf(pr.getProperty(
								"newdatabaseversion").toUpperCase());
					}

				}
				boolean useCustomOpener=pr.containsKey("jackcessopener");
				if(useCustomOpener&&pr.containsKey("memory")&&!"true".equalsIgnoreCase(pr
						.getProperty("memory")))
					
					throw new UcanaccessSQLException(ExceptionMessages.ONLY_IN_MEMORY_ALLOWED);	
				JackcessOpenerInterface jko=useCustomOpener?
						newJackcessOpenerInstance(pr.getProperty("jackcessopener")):
						new DefaultJackcessOpener();
				DBReference ref = alreadyLoaded ? as.getReference(mdb) : as
						.loadReference(mdb, ff, jko,pr.getProperty("password"));

				if (!alreadyLoaded) {

					if (pr.containsKey("memory")) {
						ref.setInMemory("true".equalsIgnoreCase(pr
								.getProperty("memory")));
					}
					
					if (pr.containsKey("showschema")) {
						ref.setShowSchema("true".equalsIgnoreCase(pr
								.getProperty("showschema")));
					}
					
					
					if (pr.containsKey("inactivitytimeout")) {
						int millis=60000*Integer.parseInt(pr.getProperty("inactivitytimeout"));
						ref.setInactivityTimeout(millis);
					}
					
					if (pr.containsKey("singleconnection")) {
						ref.setSingleConnection("true".equalsIgnoreCase(pr
								.getProperty("singleconnection")));
					}

					if (pr.containsKey("lockmdb")) {
						ref.setLockMdb("true".equalsIgnoreCase(pr
								.getProperty("lockmdb")));
					}

					if (pr.containsKey("supportsaccesslike")) {
						SQLConverter.setSupportsAccessLike("true"
								.equalsIgnoreCase(pr.getProperty("supportsaccesslike")));
					}

				}
				String pwd = ref.getDbIO().getDatabasePassword();
				if (pwd != null&&!pr.containsKey("jackcessopener")) {
					if (!pwd.equals(pr.get("password")))
						throw new UcanaccessSQLException(
								ExceptionMessages.NOT_A_VALID_PASSWORD);
					session.setPassword(pwd);
				}
				String user = pr.getProperty("user");
				if (user != null) {
					session.setUser(user);
				}
				if (pr.containsKey("ignorecase")) {
					session.setIgnoreCase("true".equalsIgnoreCase(pr
							.getProperty("ignorecase")));
				}
				SQLWarning sqlw=null;
				if (!alreadyLoaded) {
					LoadJet la=		new LoadJet(ref.getHSQLDBConnection(session), ref
							.getDbIO());
					la.loadDB();
					as.put(mdb.getAbsolutePath(), ref);
					sqlw=la.getLoadingWarnings();
				}

				UcanaccessConnection uc= new UcanaccessConnection(as.getReference(mdb), pr,
						session);
				uc.addWarnings(sqlw);
				uc.setUrl(url);
				return uc;
			} catch (Exception e) {
				throw new UcanaccessSQLException(e);
			}
		}
	}
	
	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 0;
	}

	public java.util.logging.Logger getParentLogger()
			throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties arg1)
			throws SQLException {
		return new DriverPropertyInfo[0];
	}

	public boolean jdbcCompliant() {
		return true;
	}

	private JackcessOpenerInterface newJackcessOpenerInstance(String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException, UcanaccessSQLException{
		Object newInstance=Class.forName(className).newInstance();
		if(!(newInstance instanceof JackcessOpenerInterface))
			throw new UcanaccessSQLException(ExceptionMessages.INVALID_JACKCESS_OPENER);
		return (JackcessOpenerInterface)newInstance;
	}


	private void readProperties(Properties pr, String url) {
		Properties nb=new Properties();
		
		for( Entry<Object, Object> entry:pr.entrySet()){
			String key=(String)entry.getKey();
			if(key!=null){
				nb.put(key.toLowerCase(), entry.getValue());
			}
		}
		pr.clear();
		pr.putAll(nb);
		StringTokenizer st = new StringTokenizer(url, ";");
		while (st.hasMoreTokens()) {
			String entry = st.nextToken();
			int sep;
			if ((sep = entry.indexOf("=")) > 0 && entry.length() > sep) {
				pr.put(entry.substring(0, sep).toLowerCase(), entry.substring(
						sep + 1, entry.length()));
			}
		}
	}
}
