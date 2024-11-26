package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.*;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.Table.ColumnOrder;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.FileLock;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("java:S2077") // suppress sonarcloud warnings regarding dynamically formatted SQL
public class DBReference {
    private static final String                     CIPHER_SPEC       = "AES";
    private static List<IOnReloadReferenceListener> onReloadListeners = new ArrayList<>();
    private static String                           version;

    private final Logger                            logger            = System.getLogger(getClass().getName());
    private final File                              dbFile;
    private Database                                dbIO;
    private FileLock                                fileLock          = null;
    private String                                  id                = createId();
    private boolean                                 inMemory          = true;
    private long                                    lastModified;
    private boolean                                 openExclusive     = false;
    private final MemoryTimer                       memoryTimer;
    private boolean                                 readOnly;
    private boolean                                 readOnlyFileFormat;
    private boolean                                 showSchema;
    private File                                    tempHsql;
    private File                                    toKeepHsql;
    private boolean                                 immediatelyReleaseResources;
    private boolean                                 encryptHSQLDB;
    private String                                  encryptionKey;
    private final String                            pwd;
    private final IJackcessOpenerInterface          jko;
    private Map<String, String>                     externalResourcesMapping;
    private boolean                                 firstConnection   = true;
    private FileFormat                              dbFormat;
    private boolean                                 columnOrderDisplay;
    private boolean                                 hsqldbShutdown;
    private File                                    mirrorFolder;
    private final Set<File>                         links             = new HashSet<>();
    private boolean                                 ignoreCase        = true;
    private boolean                                 mirrorReadOnly;
    private Integer                                 lobScale;
    private boolean                                 skipIndexes;
    private boolean                                 sysSchema;
    private boolean                                 preventReloading;
    private boolean                                 concatNulls;
    private boolean                                 mirrorRecreated;

    public DBReference(File fl, FileFormat ff, IJackcessOpenerInterface _jko, final String _pwd)
        throws IOException {
        dbFile = fl;
        pwd = _pwd;
        jko = _jko;
        lastModified = System.currentTimeMillis();
        memoryTimer = new MemoryTimer(this);
        if (!fl.exists() && ff != null) {
            DatabaseBuilder dbb = new DatabaseBuilder();
            dbIO = dbb.withAutoSync(false).withFileFormat(ff).withFile(fl).create();
        } else {
            dbIO = _jko.open(fl, _pwd);
            try {
                readOnlyFileFormat = dbIO.getFileFormat().equals(FileFormat.V1997);
                dbFormat = dbIO.getFileFormat();
            } catch (Exception _ignored) {
            }
            dbIO.setLinkResolver((linkerDb, linkeeFileName) -> {
                if (linkeeFileName == null) {
                    throw new IOException("Cannot resolve db link");
                }
                File linkeeFile = new File(linkeeFileName);
                Map<String, String> emr = externalResourcesMapping;
                if (!linkeeFile.exists() && emr != null && emr.containsKey(linkeeFileName.toLowerCase())) {
                    linkeeFile = new File(emr.get(linkeeFileName.toLowerCase()));
                }
                if (!linkeeFile.exists()) {
                    logger.log(Level.WARNING, "External file {0} does not exist", linkeeFile.getAbsolutePath());
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
                    closeHsqlDb(session, true);
                } catch (Exception _ex) {
                    throw new UcanaccessSQLException(_ex);
                }
                return false;
            }
        }
        return false;
    }

    public static boolean addOnReloadRefListener(IOnReloadReferenceListener _action) {
        return onReloadListeners.add(_action);
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

    Connection checkLastModified(Connection _conn, Session _session) throws Exception {
        // I'm detecting if another process(and not another thread) is writing

        if (lastModified + 2000 > filesUpdateTime() || preventReloading && !checkInside()) {
            return _conn;
        }
        updateLastModified();
        closeHsqlDb(_session);
        dbIO.flush();
        dbIO.close();
        dbIO = open(dbFile, pwd);
        id = createId();
        firstConnection = true;
        LoadJet lj = new LoadJet(getHSQLDBConnection(_session), dbIO);
        lj.setSkipIndexes(skipIndexes);
        lj.setSysSchema(sysSchema);
        lj.loadDB();

        return getHSQLDBConnection(_session);
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

    private List<File> getHSQLDBFiles() {
        if (toKeepHsql == null) {
            return List.of();
        }
        File folder = toKeepHsql.getParentFile();
        String name = toKeepHsql.getName();
        return Stream.of("data", "lck", "lobs", "log", "properties", "script")
            .map(ext -> new File(folder, name + "." + ext)).collect(Collectors.toList());
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

    private void closeHsqlDb(Session session) throws IOException {
        closeHsqlDb(session, false);
    }

    private void closeHsqlDb(Session _session, boolean _firstConnectionKeeptMirror) throws IOException {
        finalizeHsqlDb(_session);
        if (!inMemory) {
            if (toKeepHsql == null) {
                File folder = mirrorFolder == null ? dbFile.getParentFile() : mirrorFolder;
                File hbase = new File(folder, "UCanAccess_" + id);
                if (hbase.exists()) {
                    Arrays.stream(Optional.ofNullable(hbase.listFiles()).orElse(new File[0]))
                        .filter(f -> !f.delete())
                        .forEach(f -> logger.log(Level.WARNING, "Could not delete file {0}", f));
                }
                hbase.delete();
            } else if (!immediatelyReleaseResources || _firstConnectionKeeptMirror) {
                toKeepHsql.delete();
                if (toKeepHsql.createNewFile()) {
                    logger.log(Level.DEBUG, "Created file {0}", toKeepHsql);
                } else {
                    logger.log(Level.WARNING, "Could not create file {0}", toKeepHsql);
                }
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

    private void finalizeHsqlDb(Session _session) throws IOException {
        if (!hsqldbShutdown) {
            releaseLock();
            try (Connection conn = getHSQLDBConnection(_session); Statement st = conn.createStatement()) {
                st.execute("SHUTDOWN");
                hsqldbShutdown = true;
            } catch (Exception _ignored) {
            }
        }
    }

    File getDbFile() {
        return dbFile;
    }

    public Database getDbIO() {
        return dbIO;
    }

    private void setIgnoreCase(Connection _conn) {
        try (Statement st = _conn.createStatement()) {
            st.execute("SET DATABASE COLLATION \"SQL_TEXT_UCC\"");

        } catch (Exception _ignored) {

        }
    }

    private void initHSQLDB(Connection _conn) {
        try (Statement st = _conn.createStatement()) {
            st.execute("SET DATABASE SQL SYNTAX ora TRUE");
            st.execute(String.format("SET DATABASE SQL CONCAT NULLS %s", concatNulls));
            if (lobScale == null && inMemory) {
                st.execute("SET FILES LOB SCALE 1");
            } else if (lobScale != null) {
                st.execute(String.format("SET FILES LOB SCALE %s", lobScale));
            }

        } catch (Exception _ex) {
            logger.log(Level.WARNING, _ex.toString());
        }
    }

    @SuppressWarnings("java:S2095") // suppress sonarcloud warning regarding try-with-resources
    public Connection getHSQLDBConnection(Session _session) throws SQLException {
        boolean keptMirror = firstConnection && toKeepHsql != null && toKeepHsql.exists();

        Connection conn = DriverManager.getConnection(getHsqlUrl(_session),
            Optional.ofNullable(_session.getUser()).orElse("Admin"), _session.getPassword());

        if (version == null) {
            version = conn.getMetaData().getDriverVersion();
        }

        if (firstConnection) {
            if (ignoreCase && (!keptMirror || mirrorRecreated)) {
                setIgnoreCase(conn);
            }
            if (!mirrorReadOnly || !keptMirror || mirrorRecreated) {
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

    private String getKey() throws SQLException {
        if (encryptionKey == null) {
            String url = "jdbc:hsqldb:mem:" + id + "_tmp";
            try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("CALL CRYPT_KEY('" + CIPHER_SPEC + "', null) ")) {
                rs.next();
                encryptionKey = rs.getString(1);
            }
        }
        return encryptionKey;
    }

    private String getHsqlUrl(Session _session) throws SQLException {
        try {
            if (openExclusive && fileLock == null) {
                lockMdbFile();
            }
            String enc = "";
            String log = "";
            if (encryptHSQLDB) {
                enc = ";crypt_key=" + getKey() + ";crypt_type=aes;crypt_lobs=true";
            }
            if (!inMemory && toKeepHsql == null) {
                log = ";hsqldb.log_data=FALSE";
            }
            if (!inMemory && tempHsql == null) {
                if (toKeepHsql != null) {
                    if (!toKeepHsql.exists()) {
                        if (toKeepHsql.createNewFile()) {
                            logger.log(Level.DEBUG, "Created file {0}", toKeepHsql);
                        } else {
                            logger.log(Level.WARNING, "Could not create file {0}", toKeepHsql);
                        }
                    }
                    tempHsql = toKeepHsql;
                } else {
                    File folder = mirrorFolder == null ? dbFile.getParentFile() : mirrorFolder;
                    File hbase = new File(folder, "UCanAccess_" + id);
                    hbase.mkdir();
                    tempHsql = new File(hbase, id);

                    if (!tempHsql.exists()) {
                        if (tempHsql.createNewFile()) {
                            logger.log(Level.DEBUG, "Created file {0}", tempHsql);
                            tempHsql.delete();
                        } else {
                            logger.log(Level.WARNING, "Could not create file {0}", tempHsql);
                        }
                    }
                }
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        if (toKeepHsql == null) {
                            closeHsqlDb(_session);
                        } else {
                            finalizeHsqlDb(_session);
                        }
                    } catch (Exception _ex) {
                        logger.log(Level.WARNING, _ex.toString());
                    }
                }));
            }
            String mro = mirrorReadOnly ? ";readonly=true" : "";
            return "jdbc:hsqldb:" + (inMemory ? "mem:" + id : tempHsql.getAbsolutePath()) + enc + log + mro;
        } catch (IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    public long getInactivityTimeout() {
        return memoryTimer.inactivityTimeout;
    }

    private String createId() {
        return UUID.randomUUID() + "-" + new UniqueString();
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
        String suffix = FileFormat.V2016 == dbFormat || FileFormat.V2010 == dbFormat || FileFormat.V2007 == dbFormat
            ? ".laccdb"
            : ".ldb";
        return new File(folder, fileName.substring(0, suffixStart) + suffix);
    }

    private void lockMdbFile() throws UcanaccessSQLException {
        try {
            File flLock = fileLock();
            if (flLock.createNewFile()) {
                logger.log(Level.DEBUG, "Created file {0}", flLock);
            } else {
                logger.log(Level.WARNING, "Could not create file {0}", flLock);
            }

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
        } catch (IOException _ex) {
            throw new UcanaccessSQLException(_ex);
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
        for (IOnReloadReferenceListener listener : onReloadListeners) {
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
            for (IOnReloadReferenceListener listener : onReloadListeners) {
                listener.onReload();
            }
        }
        memoryTimer.timer.cancel();
        dbIO.flush();
        dbIO.close();
        closeHsqlDb(_session);

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

    /**
     * Unique string based on current date/time and a unique id.
     */
    private static final class UniqueString {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
        private static final AtomicInteger     COUNTER   = new AtomicInteger(1);
        private final String                   name;

        private UniqueString() {
            name = LocalDateTime.now().format(FORMATTER) + '_' + String.format("%03d", COUNTER.getAndIncrement());
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class MemoryTimer {
        private static final long INACTIVITY_TIMEOUT_DEFAULT = 120000;

        private final Logger      logger                     = System.getLogger(getClass().getName());
        private final DBReference dbReference;
        private final Timer       timer;
        private int               activeConnection;
        private long              inactivityTimeout          = INACTIVITY_TIMEOUT_DEFAULT;
        private long              lastConnectionTime;

        MemoryTimer(DBReference _dbReference) {
            dbReference = _dbReference;
            timer = new Timer(getClass().getSimpleName() + '-' + _dbReference.getDbFile().getName(), true);
        }

        synchronized void decrementActiveConnection(final Session _session) {
            activeConnection--;
            if (dbReference.immediatelyReleaseResources && activeConnection == 0) {
                try {
                    dbReference.shutdown(_session);
                } catch (Exception _ex) {
                    logger.log(Level.WARNING, "Error shutting down db {0}: {1}", dbReference, _ex.toString());
                }
                timer.cancel();

                return;
            }
            if (dbReference.inMemory && inactivityTimeout > 0 && activeConnection == 0) {
                TimerTask task = new TimerTask() {
                    @Override
                    public void run() {
                        synchronized (UcanaccessDriver.class) {
                            if (System.currentTimeMillis() - getLastConnectionTime() >= inactivityTimeout
                                && getActiveConnection() == 0) {
                                try {
                                    dbReference.shutdown(_session);
                                } catch (Exception _ignored) {
                                    logger.log(Level.DEBUG, "Ignore {0}", _ignored.toString());
                                }
                            }
                        }
                    }
                };
                timer.schedule(task, inactivityTimeout);
            }
        }

        synchronized int getActiveConnection() {
            return activeConnection;
        }

        synchronized long getLastConnectionTime() {
            return lastConnectionTime;
        }

        synchronized void incrementActiveConnection() {
            activeConnection++;
            if (dbReference.inMemory && inactivityTimeout > 0) {
                lastConnectionTime = System.currentTimeMillis();
            }
        }

        void setInactivityTimeout(int _inactivityTimeout) {
            inactivityTimeout = _inactivityTimeout;
        }
    }

}
