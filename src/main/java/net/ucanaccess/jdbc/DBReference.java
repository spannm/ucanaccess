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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.logging.Level;

import net.ucanaccess.converters.LoadJet;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.util.LinkResolver;

public class DBReference {
	private final static String CIPHER_SPEC = "AES";
	private static ArrayList<OnReloadReferenceListener> onReloadListeners = new ArrayList<OnReloadReferenceListener>();
	private static String version;
	private File dbFile;
	private Database dbIO;
	private FileLock fileLock = null;
	private String id = id();
	private boolean inMemory = true;
	private long lastModified;
	private boolean lockMdb = false;
	private MemoryTimer memoryTimer = new MemoryTimer();
	private boolean readOnly;
	private boolean readOnlyFileFormat;
	private boolean showSchema;
	private File tempHsql;
	private boolean singleConnection;
	private boolean encryptHSQLDB;
	private String encryptionKey;

	private class MemoryTimer {
		private final static int INACTIVITY_TIMEOUT_DEFAULT = 120000;
		private int activeConnection;
		private int inactivityTimeout = INACTIVITY_TIMEOUT_DEFAULT;
		private long lastConnectionTime;
		private Timer timer = new Timer(true);

		private synchronized void decrementActiveConnection(
				final Session session) {
			activeConnection--;
			if (DBReference.this.singleConnection && activeConnection == 0) {
				try {
					shutdown(session);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return;
			}
			if (DBReference.this.inMemory && inactivityTimeout > 0) {
				if (activeConnection == 0) {
					timer.schedule(new TimerTask() {
						@Override
						public void run() {
							try {
								synchronized (UcanaccessDriver.class) {
									if (System.currentTimeMillis()
											- getLastConnectionTime() >= inactivityTimeout
											&& getActiveConnection() == 0) {
										shutdown(session);
										System.gc();
									}
								}
							} catch (Exception e) {
							}
						}
					}, inactivityTimeout);
				}
			}
		}

		private synchronized int getActiveConnection() {
			return activeConnection;
		}

		private int getInactivityTimeout() {
			return inactivityTimeout;
		}

		private synchronized long getLastConnectionTime() {
			return lastConnectionTime;
		}

		private synchronized void incrementActiveConnection() {
			activeConnection++;
			if (DBReference.this.inMemory && inactivityTimeout > 0) {
				lastConnectionTime = System.currentTimeMillis();
			}
		}

		private void setInactivityTimeout(int inactivityTimeout) {
			this.inactivityTimeout = inactivityTimeout;
		}
	}
	
	

	public DBReference(File fl, FileFormat ff, JackcessOpenerInterface jko,
			String pwd) throws IOException, SQLException {
		this.dbFile = fl;
		this.updateLastModified();
		 silentLog();
		if (!fl.exists() && ff != null) {
			dbIO = DatabaseBuilder.create(ff, fl);
		} else {
			dbIO = jko.open(fl, pwd);
			try {
				this.readOnlyFileFormat = this.dbIO.getFileFormat().equals(
						FileFormat.V1997);
			} catch (Exception ignore) {
				// Logger.logWarning(e.getMessage());
			}
			this.dbIO.setLinkResolver(new LinkResolver() {
				public Database resolveLinkedDatabase(Database linkerDb,
						String linkeeFileName) throws IOException {
					Database ldb = open(new File(linkeeFileName));
					return ldb;
				}
			});
			dbIO.setEnforceForeignKeys(false);
		}
	}

	public static Database open(File dbfl) throws IOException {
		silentLog();
		DatabaseBuilder dbb = new DatabaseBuilder(dbfl);
		dbb.setAutoSync(false);
		Database db;
		try {
			dbb.setReadOnly(false);
			db = dbb.open();
		} catch (IOException e) {
			dbb.setReadOnly(true);
			db = dbb.open();
		}
		db.setEnforceForeignKeys(false);
		return db;
	}

	public static boolean addOnReloadRefListener(
			OnReloadReferenceListener action) {
		return onReloadListeners.add(action);
	}

	public static String getVersion() {
		return version;
	}

	public static boolean is2xx() {
		return version.startsWith("2.");
	}

	Connection checkLastModified(Connection conn, Session session)
			throws Exception {
		// I'm detecting if another process(and not another thread) is writing
		for (int i = 0; i < Thread.activeCount(); i++) {
			if (lastModified >= this.dbFile.lastModified()) {
				return conn;
			} else {
				Thread.sleep(10);
			}
		}
		this.updateLastModified();
		this.closeHSQLDB(session);
		this.dbIO.flush();
		this.dbIO.close();
		this.dbIO =open(this.dbFile);
		this.id = id();
		new LoadJet(getHSQLDBConnection(session), dbIO).loadDB();
		return getHSQLDBConnection(session);
	}

	private void closeHSQLDB(Session session) throws Exception {
		finalizeHSQLDB(session);
		if (!this.inMemory) {
			File folder = dbFile.getParentFile();
			File hbase = new File(folder, "Ucanaccess_" + this);
			for (File hsqlF : hbase.listFiles()) {
				hsqlF.delete();
			}
			hbase.delete();
		}
	}

	public void decrementActiveConnection(Session session) {
		memoryTimer.decrementActiveConnection(session);
	}
	
	private static void silentLog(){
		java.util.logging.Logger logger = java.util.logging.Logger
		.getLogger("com.healthmarketscience.jackcess");
         logger.setLevel(Level.OFF);
	}

	private void finalizeHSQLDB(Session session) throws Exception {
		this.releaseLock();
		Connection conn = null;
		Statement st = null;
		try {
			conn = this.getHSQLDBConnection(session);
			st = conn.createStatement();
			st.execute("SHUTDOWN");
		} catch (Exception w) {
		} finally {
			if (st != null)
				st.close();
			if (conn != null)
				conn.close();
		}
	}

	File getDbFile() {
		return dbFile;
	}

	public Database getDbIO() {
		return dbIO;
	}

	public Connection getHSQLDBConnection(Session session) throws SQLException {
		Connection conn;
		conn = DriverManager.getConnection(this.getHsqlUrl(session),
				session.getUser() == null ? "Admin" : session.getUser(),
				session.getPassword());
		if (version == null) {
			version = conn.getMetaData().getDriverVersion();
		}
		if (session.isIgnoreCase()) {
			Statement st = null;
			try {
				st = conn.createStatement();
				st.execute("set ignorecase true");
			} catch (Exception w) {
			} finally {
				if (st != null)
					st.close();
			}
		}
		conn.setAutoCommit(false);
		return conn;
	}

	private String key(String pwd) throws SQLException {
		if (this.encryptionKey == null) {
			String url = "jdbc:hsqldb:mem:" + id + "_tmp";
			Connection conn = DriverManager.getConnection(url);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("CALL  CRYPT_KEY('" + CIPHER_SPEC
					+ "', null) ");
			rs.next();
			this.encryptionKey = rs.getString(1);
		}
		return this.encryptionKey;
	}

	private String getHsqlUrl(final Session session) throws SQLException {
		try {
			if (this.lockMdb && this.fileLock == null) {
				lockMdbFile();
			}
			String enc = "";
			if (this.encryptHSQLDB) {
				enc = ";crypt_key=" + key("AES")
						+ ";crypt_type=aes;crypt_lobs=true";
			}
			if (!this.inMemory && tempHsql == null) {
				File folder = dbFile.getParentFile();
				File hbase = new File(folder, "Ucanaccess_" + toString());
				hbase.mkdir();
				this.tempHsql = new File(hbase, this.id);
				this.tempHsql.createNewFile();
				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
					public void run() {
						try {
							closeHSQLDB(session);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}));
			}
			return "jdbc:hsqldb:"
					+ (this.inMemory ? "mem:" + id : tempHsql.getAbsolutePath())
					+ enc;
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public int getInactivityTimeout() {
		return memoryTimer.getInactivityTimeout();
	}

	private String id() {
		return UUID.randomUUID() + toString();
	}

	public void incrementActiveConnection() {
		memoryTimer.incrementActiveConnection();
	}

	public boolean isReadOnly() throws UcanaccessSQLException {
		if (readOnly) {
			lockMdbFile();
		}
		return this.readOnlyFileFormat || readOnly;
	}

	boolean isShowSchema() {
		return showSchema;
	}

	private void lockMdbFile() throws UcanaccessSQLException {
		try {
			File folder = dbFile.getParentFile();
			String fileName = dbFile.getName();
			int suffixStart = fileName.lastIndexOf('.');
			if (suffixStart < 0)
				suffixStart = fileName.length();
			File flLock = new File(folder, fileName.substring(0, suffixStart)
					+ ".ldb");
			flLock.createNewFile();
			final RandomAccessFile raf = new RandomAccessFile(flLock, "rw");
			FileLock tryLock = raf.getChannel().tryLock();
			if (tryLock == null) {
				this.readOnly = true;
			} else {
				this.fileLock = tryLock;
				this.readOnly = false;
			}
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public void releaseLock() throws IOException {
		if (this.fileLock != null) {
			this.fileLock.release();
		}
	}

	public void reloadDbIO() throws IOException {
		this.dbIO.close();
		for (OnReloadReferenceListener listener : onReloadListeners) {
			listener.onReload();
		}
		this.dbIO = open(dbFile);
	}

	void setDbAccess(Database dbAccess) {
		this.dbIO = dbAccess;
	}

	void setDbFile(File dbFile) {
		this.dbFile = dbFile;
	}

	public void setInactivityTimeout(int inactivityTimeout) {
		memoryTimer.setInactivityTimeout(inactivityTimeout);
	}

	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}

	public void setLockMdb(boolean lockMdb) {
		this.lockMdb = lockMdb;
	}

	public void setShowSchema(boolean showSchema) {
		this.showSchema = showSchema;
	}

	void setTempHsql(File tempHsql) {
		this.tempHsql = tempHsql;
	}

	void shutdown(Session session) throws Exception {
		DBReferenceSingleton.getInstance()
				.remove(this.dbFile.getAbsolutePath());
		this.dbIO.flush();
		this.dbIO.close();
		this.closeHSQLDB(session);
	}

	public void updateLastModified() {
		this.lastModified = this.dbFile.lastModified();
	}

	public void setSingleConnection(boolean singleConnection) {
		this.singleConnection = singleConnection;
	}

	public void setEncryptHSQLDB(boolean encryptHSQLDB) {
		this.encryptHSQLDB = encryptHSQLDB;
	}
}
