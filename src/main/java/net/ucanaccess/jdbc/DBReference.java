package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;
import io.github.spannm.jackcess.Row;
import io.github.spannm.jackcess.Table;
import io.github.spannm.jackcess.Table.ColumnOrder;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.Metadata;
import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.file.AccessDeniedException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private Charset                                 charset;
    /**
     * Whether the {@link io.github.spannm.jackcess.util.LinkResolver} registered below may automatically resolve
     * linked tables that point to a network/UNC path. {@code false} by default; see
     * {@link net.ucanaccess.converters.Metadata.Property#allowRemoteLinks}.
     */
    private boolean                                 allowRemoteLinks;

    public DBReference(File fl, FileFormat ff, IJackcessOpenerInterface jko, final String pwd, Charset charset)
        throws IOException {
        dbFile = fl;
        this.pwd = pwd;
        this.jko = jko;
        this.charset = charset;
        lastModified = System.currentTimeMillis();
        memoryTimer = new MemoryTimer(this);
        if (!fl.exists() && ff != null) {
            DatabaseBuilder dbb = new DatabaseBuilder();
            dbIO = dbb.withAutoSync(false).withFileFormat(ff).withFile(fl).create();
        } else {
            dbIO = jko.open(fl, pwd, charset);
            try {
                readOnlyFileFormat = dbIO.getFileFormat().equals(FileFormat.V1997);
                dbFormat = dbIO.getFileFormat();
            } catch (Exception ignored) {
            }
            dbIO.setLinkResolver((linkerDb, linkeeFileName) -> {
                if (linkeeFileName == null) {
                    throw new IOException("Cannot resolve db link");
                }
                Map<String, String> emr = externalResourcesMapping;
                String remapped = emr == null ? null : emr.get(linkeeFileName.toLowerCase());
                // an explicit remapping via the "reMap" connection property is trusted application configuration
                // and therefore bypasses the network-path guard below; the raw (untrusted) path stored in the
                // database file itself is not probed or opened in that case
                if (remapped == null && !allowRemoteLinks && isNetworkPath(linkeeFileName)) {
                    throw new AccessDeniedException(linkeeFileName, null,
                        "Linked database points to a network/UNC path; automatic resolution of such paths is "
                            + "disabled by default because the linked path is taken verbatim from the (possibly "
                            + "untrusted) Access database file. Set the '" + Metadata.Property.allowRemoteLinks
                            + "' connection property to true only if linked network paths are trusted.");
                }
                // for a network path with an explicit remapping, use the remapped (trusted) path directly without
                // probing the raw network path first
                File linkeeFile = remapped != null && isNetworkPath(linkeeFileName)
                    ? new File(remapped)
                    : new File(linkeeFileName);
                if (!linkeeFile.exists() && remapped != null) {
                    linkeeFile = new File(remapped);
                }
                if (!linkeeFile.exists()) {
                    logger.log(Level.WARNING, "External file {0} does not exist", linkeeFile.getAbsolutePath());
                } else {
                    links.add(linkeeFile);
                }
                Database ldb = open(linkeeFile, pwd);
                ldb.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
                return ldb;
            });
            dbIO.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
            dbIO.setEnforceForeignKeys(false);
        }
    }

    /**
     * Determines whether the given linked database path is a network/UNC path, e.g. {@code \\server\share\db.accdb},
     * {@code //server/share/db.accdb} or a device-style UNC path such as {@code \\?\UNC\server\share\db.accdb}.
     * <p>
     * Such paths are excluded from automatic resolution by default since they let an untrusted Access database
     * file trigger an outbound network connection (see {@link #allowRemoteLinks}).
     *
     * @param  fileName the linked database path as stored in the Access database file
     * @return           {@code true} if the path is a network/UNC path, {@code false} otherwise
     */
    static boolean isNetworkPath(String fileName) {
        String normalized = fileName.replace('/', '\\');
        return normalized.startsWith("\\\\");
    }

    public Database open(File dbfl, String password) throws IOException {
        Database ret = jko.open(dbfl, password, charset);
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
                } catch (Exception ex) {
                    throw new UcanaccessSQLException(ex);
                }
                return false;
            }
        }
        return false;
    }

    public static boolean addOnReloadRefListener(IOnReloadReferenceListener action) {
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

    Connection checkLastModified(Connection conn, Session session) throws UcanaccessSQLException {
        // I'm detecting if another process(and not another thread) is writing

        try {
            if (lastModified + 2000 > filesUpdateTime() || preventReloading && !checkInside()) {
                return conn;
            }
            updateLastModified();
            closeHsqlDb(session);
            dbIO.flush();
            dbIO.close();
            dbIO = open(dbFile, pwd);
            id = createId();
            firstConnection = true;
            LoadJet lj = new LoadJet(getHSQLDBConnection(session), dbIO);
            lj.setSkipIndexes(skipIndexes);
            lj.setSysSchema(sysSchema);
            lj.loadDB();

            return getHSQLDBConnection(session);
        } catch (SQLException | IOException ex) {
            throw UcanaccessSQLException.wrap(ex);
        }
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

    private void closeHsqlDb(Session session, boolean firstConnectionKeeptMirror) throws IOException {
        finalizeHsqlDb(session);
        if (!inMemory) {
            if (toKeepHsql == null) {
                File folder = mirrorFolder == null ? dbFile.getParentFile() : mirrorFolder;
                File hbase = new File(folder, "UCanAccess_" + id);
                if (hbase.exists()) {
                    Arrays.stream(Optional.ofNullable(hbase.listFiles()).orElse(new File[0]))
                        .filter(f -> !f.delete())
                        .forEach(f -> logger.log(Level.WARNING, "Could not delete {0}", f));
                }
                boolean deleted = hbase.delete();
                if (!deleted) {
                    logger.log(Level.INFO, "Could not delete {0}", hbase);
                }

            } else if (!immediatelyReleaseResources || firstConnectionKeeptMirror) {
                boolean deleted = toKeepHsql.delete();
                if (!deleted) {
                    logger.log(Level.INFO, "Could not delete {0}", toKeepHsql);
                }
                if (toKeepHsql.createNewFile()) {
                    logger.log(Level.DEBUG, "Created file {0}", toKeepHsql);
                } else {
                    logger.log(Level.WARNING, "Could not create file {0}", toKeepHsql);
                }
                for (File hsqlf : getHSQLDBFiles()) {
                    if (hsqlf.exists() && !hsqlf.delete()) {
                        logger.log(Level.INFO, "Could not delete {0}", hsqlf);
                    }
                }
                mirrorRecreated = true;
            }
        }

    }

    public void decrementActiveConnection(Session session) {
        memoryTimer.decrementActiveConnection(session);
    }

    private void finalizeHsqlDb(Session session) throws IOException {
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
            st.execute(String.format("SET DATABASE SQL CONCAT NULLS %s", concatNulls));
            if (lobScale == null && inMemory) {
                st.execute("SET FILES LOB SCALE 1");
            } else if (lobScale != null) {
                st.execute(String.format("SET FILES LOB SCALE %s", lobScale));
            }

        } catch (Exception ex) {
            logger.log(Level.WARNING, ex.toString());
        }
    }

    public Connection getHSQLDBConnection(Session session) throws SQLException {
        boolean keptMirror = firstConnection && toKeepHsql != null && toKeepHsql.exists();

        Connection conn = DriverManager.getConnection(getHsqlUrl(session),
            Optional.ofNullable(session.getUser()).orElse("Admin"), session.getPassword());

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

    private String getHsqlUrl(Session session) throws SQLException {
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
                            if (!tempHsql.delete()) {
                                logger.log(Level.INFO, "Could not delete {0}", tempHsql);
                            }
                        } else {
                            logger.log(Level.WARNING, "Could not create file {0}", tempHsql);
                        }
                    }
                }
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        if (toKeepHsql == null) {
                            closeHsqlDb(session);
                        } else {
                            finalizeHsqlDb(session);
                        }
                    } catch (Exception ex) {
                        logger.log(Level.WARNING, ex.toString());
                    }
                }));
            }
            String mro = mirrorReadOnly ? ";readonly=true" : "";
            return "jdbc:hsqldb:" + (inMemory ? "mem:" + id : tempHsql.getAbsolutePath()) + enc + log + mro;
        } catch (IOException ex) {
            throw new UcanaccessSQLException(ex);
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
        } catch (IOException ex) {
            throw new UcanaccessSQLException(ex);
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

    public void setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
    }

    public void setOpenExclusive(boolean openExclusive) {
        this.openExclusive = openExclusive;
    }

    public void setShowSchema(boolean showSchema) {
        this.showSchema = showSchema;
    }

    void shutdown(Session session) throws Exception {
        DBReferenceSingleton.getInstance().remove(dbFile.getAbsolutePath());
        if (immediatelyReleaseResources) {
            for (IOnReloadReferenceListener listener : onReloadListeners) {
                listener.onReload();
            }
        }
        memoryTimer.timer.cancel();
        dbIO.flush();
        dbIO.close();
        closeHsqlDb(session);

    }

    public void updateLastModified() {
        lastModified = filesUpdateTime();
    }

    public void setImmediatelyReleaseResources(boolean immediatelyReleaseResources) {
        this.immediatelyReleaseResources = immediatelyReleaseResources;
    }

    public void setEncryptHSQLDB(boolean encryptHSQLDB) {
        this.encryptHSQLDB = encryptHSQLDB;
    }

    public void setExternalResourcesMapping(Map<String, String> externalResourcesMapping) {
        this.externalResourcesMapping = externalResourcesMapping;
    }

    /**
     * Enables or disables automatic resolution of linked tables that point to a network/UNC path.
     * <p>
     * Disabled by default; see the security note on {@link net.ucanaccess.converters.Metadata.Property#allowRemoteLinks}
     * for why this matters when opening database files from an untrusted source.
     *
     * @param allowRemoteLinks {@code true} to allow automatic resolution of network/UNC linked paths
     */
    public void setAllowRemoteLinks(boolean allowRemoteLinks) {
        this.allowRemoteLinks = allowRemoteLinks;
    }

    public File getToKeepHsql() {
        return toKeepHsql;
    }

    public void setToKeepHsql(File toKeepHsql) {
        this.toKeepHsql = toKeepHsql;
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

    public void setMirrorFolder(File mirrorFolder) {
        this.mirrorFolder = mirrorFolder;
    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public void setMirrorReadOnly(boolean mirrorReadOnly) {
        this.mirrorReadOnly = mirrorReadOnly;
    }

    public void setLobScale(Integer lobScale) {
        this.lobScale = lobScale;
    }

    public void setSkipIndexes(boolean skipIndexes) {
        this.skipIndexes = skipIndexes;
    }

    public void setSysSchema(boolean sysSchema) {
        this.sysSchema = sysSchema;
    }

    public boolean isPreventReloading() {
        return preventReloading;
    }

    public void setPreventReloading(boolean preventReloading) {
        this.preventReloading = preventReloading;
    }

    public boolean isConcatNulls() {
        return concatNulls;
    }

    public void setConcatNulls(boolean concatNulls) {
        this.concatNulls = concatNulls;
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

        MemoryTimer(DBReference dbReference) {
            this.dbReference = dbReference;
            timer = new Timer(getClass().getSimpleName() + '-' + dbReference.getDbFile().getName(), true);
        }

        synchronized void decrementActiveConnection(final Session session) {
            activeConnection--;
            if (dbReference.immediatelyReleaseResources && activeConnection == 0) {
                try {
                    dbReference.shutdown(session);
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "Error shutting down db {0}: {1}", dbReference, ex.toString());
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
                                    dbReference.shutdown(session);
                                } catch (Exception ignored) {
                                    logger.log(Level.DEBUG, "Ignore {0}", ignored.toString());
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

        void setInactivityTimeout(int inactivityTimeout) {
            this.inactivityTimeout = inactivityTimeout;
        }
    }

}
