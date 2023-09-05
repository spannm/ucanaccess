package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Table.ColumnOrder;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.util.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class DBReference {
    private static final String                    CIPHER_SPEC       = "AES";
    private static List<OnReloadReferenceListener> onReloadListeners = new ArrayList<>();
    private static String                          version;
    private File                                   dbFile;
    private Database                               dbIO;
    private FileLock                               fileLock          = null;
    private String                                 id                = id();
    private boolean                                inMemory          = true;
    private long                                   lastModified;
    private boolean                                openExclusive     = false;
    private MemoryTimer                            memoryTimer;
    private boolean                                readOnly;
    private boolean                                readOnlyFileFormat;
    private boolean                                showSchema;
    private File                                   tempHsql;
    private File                                   toKeepHsql;
    private boolean                                immediatelyReleaseResources;
    private boolean                                encryptHSQLDB;
    private String                                 encryptionKey;
    private String                                 pwd;
    private JackcessOpenerInterface                jko;
    private Map<String, String>                    externalResourcesMapping;
    private boolean                                firstConnection   = true;
    private FileFormat                             dbFormat;
    private boolean                                columnOrderDisplay;
    private boolean                                hsqldbShutdown;
    private File                                   mirrorFolder;
    private Set<File>                              links             = new HashSet<>();
    private boolean                                ignoreCase        = true;
    private boolean                                mirrorReadOnly;
    private Integer                                lobScale;
    private boolean                                skipIndexes;
    private boolean                                sysSchema;
    private boolean                                preventReloading;
    private boolean                                concatNulls;
    private boolean                                mirrorRecreated;

    private static class MemoryTimer {
        private static final long INACTIVITY_TIMEOUT_DEFAULT = 120000;

        private final DBReference dbReference;
        private final Timer       timer;
        private int               activeConnection;
        private long              inactivityTimeout          = INACTIVITY_TIMEOUT_DEFAULT;
        private long              lastConnectionTime;

        MemoryTimer(DBReference _dbReference) {
            dbReference = _dbReference;
            timer = new Timer(getClass().getSimpleName() + '-' + _dbReference.getDbFile().getName(), true);
        }

        private synchronized void decrementActiveConnection(final Session _session) {
            activeConnection--;
            if (dbReference.immediatelyReleaseResources && activeConnection == 0) {
                try {

                    dbReference.shutdown(_session);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }
            if (dbReference.inMemory && inactivityTimeout > 0) {
                if (activeConnection == 0) {
                    TimerTask task = new TimerTask() {
                        @Override
                        public void run() {
                            synchronized (UcanaccessDriver.class) {
                                if (System.currentTimeMillis() - getLastConnectionTime() >= inactivityTimeout
                                    && getActiveConnection() == 0) {
                                    try {
                                        dbReference.shutdown(_session);
                                    } catch (Exception ignored) {}
                                    System.gc();
                                }
                            }
                        }
                    };
                    timer.schedule(task, inactivityTimeout);
                }
            }
        }

        private synchronized int getActiveConnection() {
            return activeConnection;
        }

        private long getInactivityTimeout() {
            return inactivityTimeout;
        }

        private synchronized long getLastConnectionTime() {
            return lastConnectionTime;
        }

        private synchronized void incrementActiveConnection() {
            activeConnection++;
            if (dbReference.inMemory && inactivityTimeout > 0) {
                lastConnectionTime = System.currentTimeMillis();
            }
        }

        private void setInactivityTimeout(int _inactivityTimeout) {
            inactivityTimeout = _inactivityTimeout;
        }
    }

    public DBReference(File fl, FileFormat ff, JackcessOpenerInterface _jko, final String _pwd)
        throws IOException {
        dbFile = fl;
        pwd = _pwd;
        jko = _jko;
        lastModified = System.currentTimeMillis();
        memoryTimer = new MemoryTimer(this);
        Logger.turnOffJackcessLog();
        if (!fl.exists() && ff != null) {
            DatabaseBuilder dbb = new DatabaseBuilder();
            dbIO = dbb.setAutoSync(false).setFileFormat(ff).setFile(fl).create();
        } else {
            dbIO = _jko.open(fl, _pwd);
            try {
                readOnlyFileFormat = dbIO.getFileFormat().equals(FileFormat.V1997);
                dbFormat = dbIO.getFileFormat();
            } catch (Exception ignore) {

            }
            dbIO.setLinkResolver((linkerDb, linkeeFileName) -> {
                if (linkeeFileName == null) {
                    throw new IOException("Cannot resolve db link");
                }
                File linkeeFile = new File(linkeeFileName);
                Map<String, String> emr = DBReference.this.externalResourcesMapping;
                if (!linkeeFile.exists() && emr != null && emr.containsKey(linkeeFileName.toLowerCase())) {
                    linkeeFile = new File(emr.get(linkeeFileName.toLowerCase()));
                }
                if (!linkeeFile.exists()) {
                    Logger.logWarning("External file " + linkeeFile.getAbsolutePath() + " does not exist");
                } else {
                    links.add(linkeeFile);
                }
                Database ldb = open(linkeeFile, _pwd);
                ldb.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
                return ldb;
            });
            dbIO.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
            dbIO.setEnforceForeignKeys(false);
        }
    }

    public Database open(File _dbfl, String _pwd) throws IOException {
        Logger.turnOffJackcessLog();
        Database ret = jko.open(_dbfl, _pwd);
        if (columnOrderDisplay) {
            ret.setColumnOrder(ColumnOrder.DISPLAY);
        }
        return ret;
    }

    boolean loadedFromKeptMirror(Session session) throws UcanaccessSQLException {
        if (toKeepHsql != null && toKeepHsql.exists()) {
            if (getLastUpdateHSQLDB() >= dbFile.lastModified()) {
                return true;
            } else {
                try {
                    closeHSQLDB(session, true);
                } catch (Exception e) {
                    throw new UcanaccessSQLException(e);
                }
                return false;
            }
        }
        return false;
    }

    public static boolean addOnReloadRefListener(OnReloadReferenceListener action) {
        return onReloadListeners.add(action);
    }

    public static String getVersion() {
        return version;
    }

    public static boolean is2xx() {
        return version.startsWith("2.");
    }

    private long filesUpdateTime() {
        long lm = dbFile.lastModified();
        for (File fl : links) {
            lm = Math.max(lm, fl.lastModified());
        }
        return lm;
    }

    Connection checkLastModified(Connection conn, Session session) throws Exception {
        // I'm detecting if another process(and not another thread) is writing

        if ((lastModified + 2000 > filesUpdateTime()) || (preventReloading && !checkInside())) {
            return conn;
        }
        updateLastModified();
        closeHSQLDB(session);
        System.gc();
        dbIO.flush();
        dbIO.close();
        dbIO = open(dbFile, pwd);
        id = id();
        firstConnection = true;
        LoadJet lj = new LoadJet(getHSQLDBConnection(session), dbIO);
        lj.setSkipIndexes(skipIndexes);
        lj.setSysSchema(sysSchema);
        lj.loadDB();

        return getHSQLDBConnection(session);
    }

    private boolean checkInside(Database db) throws IOException {
        Table t = db.getSystemTable("MSysObjects");

        for (Row row : t) {
            Object dobj = row.get("DateUpdate");
            Object tobj = row.get("Type");

            if (dobj == null || tobj == null) {
                continue;
            }
            Date dt = (Date) dobj;

            short type = (Short) tobj;
            if (lastModified < dt.getTime() && (type == 1 || type == 5 || type == 8)

            ) {
                return true;
            }

        }
        return false;
    }

    private boolean checkInside() throws IOException {

        boolean reload = checkInside(dbIO);
        if (reload) {
            return true;
        }
        for (File fl : links) {
            Database db = DatabaseBuilder.open(fl);
            reload = checkInside(db);
            db.close();
            if (reload) {
                return true;
            }
        }

        return false;
    }

    private File[] getHSQLDBFiles() {
        if (toKeepHsql == null) {
            return new File[] {};
        }
        File folder = toKeepHsql.getParentFile();
        String name = toKeepHsql.getName();
        return new File[] {new File(folder, name + ".data"), new File(folder, name + ".script"), new File(folder, name + ".properties"), new File(folder, name + ".log"), new File(folder,
            name + ".lck"), new File(folder, name + ".lobs")};
    }

    private long getLastUpdateHSQLDB() {
        long lu = 0;
        for (File hsqlF : getHSQLDBFiles()) {
            if (hsqlF.exists() && hsqlF.lastModified() > lu) {
                lu = hsqlF.lastModified();
            }
        }
        if (toKeepHsql != null && toKeepHsql.exists() && toKeepHsql.lastModified() > lu) {
            lu = toKeepHsql.lastModified();
        }
        return lu;
    }

    private void closeHSQLDB(Session session) throws Exception {
        closeHSQLDB(session, false);
    }

    private void closeHSQLDB(Session session, boolean firstConnectionKeeptMirror) throws Exception {
        finalizeHSQLDB(session);
        if (!inMemory) {
            if (toKeepHsql == null) {
                File folder = mirrorFolder == null ? dbFile.getParentFile() : mirrorFolder;
                File hbase = new File(folder, "Ucanaccess_" + this);
                if (hbase.exists()) {
                    for (File hsqlF : hbase.listFiles()) {
                        hsqlF.delete();
                    }
                }
                hbase.delete();
            } else if (!immediatelyReleaseResources || firstConnectionKeeptMirror) {
                toKeepHsql.delete();
                toKeepHsql.createNewFile();
                for (File hsqlf : getHSQLDBFiles()) {
                    if (hsqlf.exists()) {
                        hsqlf.delete();
                    }
                }
                mirrorRecreated = true;

            }
        }

    }

    public void decrementActiveConnection(Session session) {
        memoryTimer.decrementActiveConnection(session);
    }

    private void finalizeHSQLDB(Session session) throws Exception {
        if (!hsqldbShutdown) {
            releaseLock();
            try (Connection conn = getHSQLDBConnection(session); Statement st = conn.createStatement()) {
                st.execute("SHUTDOWN");
                hsqldbShutdown = true;
            } catch (Exception ignored) {
            }
        }
    }

    File getDbFile() {
        return dbFile;
    }

    public Database getDbIO() {
        return dbIO;
    }

    private void setIgnoreCase(Connection conn) {
        try (Statement st = conn.createStatement()) {
            st.execute("SET DATABASE COLLATION \"SQL_TEXT_UCC\"");

        } catch (Exception ignored) {

        }
    }

    private void initHSQLDB(Connection conn) {
        try (Statement st = conn.createStatement()) {
            st.execute("SET DATABASE SQL SYNTAX ora TRUE");
            st.execute("SET DATABASE SQL CONCAT NULLS " + concatNulls);
            if (lobScale == null && inMemory) {
                st.execute("SET FILES LOB SCALE 1");
            } else if (lobScale != null) {
                st.execute("SET FILES LOB SCALE " + lobScale);
            }

        } catch (Exception w) {
            w.printStackTrace();
        }
    }

    public Connection getHSQLDBConnection(Session session) throws SQLException {

        boolean keptMirror = false;
        if (firstConnection && toKeepHsql != null && toKeepHsql.exists()) {
            keptMirror = true;
        }

        Connection conn = DriverManager.getConnection(getHsqlUrl(session),
            session.getUser() == null ? "Admin" : session.getUser(), session.getPassword());
        if (version == null) {
            version = conn.getMetaData().getDriverVersion();
        }

        if (firstConnection) {
            if (ignoreCase && (!keptMirror || mirrorRecreated)) {
                setIgnoreCase(conn);
            }
            if (!mirrorReadOnly || (!keptMirror || mirrorRecreated)) {
                initHSQLDB(conn);
            }
            firstConnection = false;
            mirrorRecreated = false;
        }
        hsqldbShutdown = false;
        conn.setAutoCommit(false);
        return conn;
    }

    String getId() {
        return id;
    }

    private String key(String _pwd) throws SQLException {
        Connection conn = null;
        try {
            if (encryptionKey == null) {
                String url = "jdbc:hsqldb:mem:" + id + "_tmp";
                conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("CALL  CRYPT_KEY('" + CIPHER_SPEC + "', null) ");
                rs.next();
                encryptionKey = rs.getString(1);
            }
            return encryptionKey;
        } finally {
            if (conn != null) {
                conn.close();
            }

        }
    }

    private String getHsqlUrl(final Session session) throws SQLException {
        try {
            if (openExclusive && fileLock == null) {
                lockMdbFile();
            }
            String enc = "";
            String log = "";
            if (encryptHSQLDB) {
                enc = ";crypt_key=" + key("AES") + ";crypt_type=aes;crypt_lobs=true";
            }
            if (!inMemory && toKeepHsql == null) {
                log = ";hsqldb.log_data=FALSE";
            }
            if (!inMemory && tempHsql == null) {
                if (toKeepHsql != null) {
                    if (!toKeepHsql.exists()) {
                        toKeepHsql.createNewFile();
                    }
                    tempHsql = toKeepHsql;
                } else {
                    File folder = mirrorFolder == null ? dbFile.getParentFile() : mirrorFolder;
                    File hbase = new File(folder, "Ucanaccess_" + this);
                    hbase.mkdir();
                    tempHsql = new File(hbase, id);

                    tempHsql.createNewFile();
                }
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        if (toKeepHsql == null) {
                            closeHSQLDB(session);
                        } else {
                            finalizeHSQLDB(session);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
            }
            String mro = mirrorReadOnly ? ";readonly=true" : "";
            return "jdbc:hsqldb:" + (inMemory ? "mem:" + id : tempHsql.getAbsolutePath()) + enc + log + mro;
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public long getInactivityTimeout() {
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
        return readOnlyFileFormat || readOnly;
    }

    boolean isReadOnlyFileFormat() {
        return readOnlyFileFormat;
    }

    boolean isShowSchema() {
        return showSchema;
    }

    private File fileLock() {
        File folder = dbFile.getParentFile();
        String fileName = dbFile.getName();
        int suffixStart = fileName.lastIndexOf('.');
        if (suffixStart < 0) {
            suffixStart = fileName.length();
        }
        String suffix = (FileFormat.V2016.equals(dbFormat) || FileFormat.V2010.equals(dbFormat) || FileFormat.V2007.equals(dbFormat))
                ? ".laccdb"
                : ".ldb";
        return new File(folder, fileName.substring(0, suffixStart) + suffix);
    }

    private void lockMdbFile() throws UcanaccessSQLException {
        try {
            File flLock = fileLock();
            flLock.createNewFile();
            // suppress Eclipse warning "Resource leak: 'raf' is never closed", because that is exactly how UCanAccess
            // "locks" the file
            @SuppressWarnings("resource")
            final RandomAccessFile raf = new RandomAccessFile(flLock, "rw");
            FileLock tryLock = raf.getChannel().tryLock();
            if (tryLock == null) {
                readOnly = true;
            } else {
                fileLock = tryLock;
                readOnly = false;
            }
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    public void releaseLock() throws IOException {
        if (fileLock != null) {
            fileLock.release();
        }
    }

    public void reloadDbIO() throws IOException {
        dbIO.flush();
        dbIO.close();
        for (OnReloadReferenceListener listener : onReloadListeners) {
            listener.onReload();
        }
        dbIO = open(dbFile, pwd);

    }

    public void setInactivityTimeout(int inactivityTimeout) {
        memoryTimer.setInactivityTimeout(inactivityTimeout);
    }

    public void setInMemory(boolean _inMemory) {
        inMemory = _inMemory;
    }

    public void setOpenExclusive(boolean _openExclusive) {
        openExclusive = _openExclusive;
    }

    public void setShowSchema(boolean _showSchema) {
        showSchema = _showSchema;
    }

    void shutdown(Session _session) throws Exception {
        DBReferenceSingleton.getInstance().remove(dbFile.getAbsolutePath());
        if (immediatelyReleaseResources) {
            for (OnReloadReferenceListener listener : onReloadListeners) {
                listener.onReload();
            }
        }
        memoryTimer.timer.cancel();
        dbIO.flush();
        dbIO.close();
        closeHSQLDB(_session);

    }

    public void updateLastModified() {
        lastModified = filesUpdateTime();
    }

    public void setImmediatelyReleaseResources(boolean _immediatelyReleaseResources) {
        immediatelyReleaseResources = _immediatelyReleaseResources;
    }

    public void setEncryptHSQLDB(boolean _encryptHSQLDB) {
        encryptHSQLDB = _encryptHSQLDB;
    }

    public void setExternalResourcesMapping(Map<String, String> _externalResourcesMapping) {
        externalResourcesMapping = _externalResourcesMapping;
    }

    public File getToKeepHsql() {
        return toKeepHsql;
    }

    public void setToKeepHsql(File _toKeepHsql) {
        toKeepHsql = _toKeepHsql;
    }

    public boolean isEncryptHSQLDB() {
        return encryptHSQLDB;
    }

    public void setColumnOrderDisplay() {
        columnOrderDisplay = true;
        if (dbIO != null) {
            dbIO.setColumnOrder(ColumnOrder.DISPLAY);
        }
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public void setMirrorFolder(File _mirrorFolder) {
        mirrorFolder = _mirrorFolder;

    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean _ignoreCase) {
        ignoreCase = _ignoreCase;
    }

    public void setMirrorReadOnly(boolean _mirrorReadOnly) {
        mirrorReadOnly = _mirrorReadOnly;
    }

    public void setLobScale(Integer _lobScale) {
        lobScale = _lobScale;
    }

    public void setSkipIndexes(boolean _skipIndexes) {
        skipIndexes = _skipIndexes;
    }

    public void setSysSchema(boolean _sysSchema) {
        sysSchema = _sysSchema;
    }

    public boolean isPreventReloading() {
        return preventReloading;
    }

    public void setPreventReloading(boolean _preventReloading) {
        preventReloading = _preventReloading;
    }

    public boolean isConcatNulls() {
        return concatNulls;
    }

    public void setConcatNulls(boolean _concatNulls) {
        concatNulls = _concatNulls;
    }

    //CHECKSTYLE:OFF
    @Override
    protected void finalize() throws Throwable {
        if (memoryTimer != null) {
            memoryTimer.timer.cancel();
            memoryTimer = null;
        }
        super.finalize();
    }
    //CHECKSTYLE:ON

}
