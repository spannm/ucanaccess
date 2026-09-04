package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;

import io.github.spannm.jackcess.Database.FileFormat;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.exception.AuthenticationException;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.type.ColumnOrder;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public final class UcanaccessDriver implements Driver {

    public static final String  URL_PREFIX = "jdbc:ucanaccess://";

    private static final Logger LOGGER     = System.getLogger(UcanaccessDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new UcanaccessDriver());
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            // Set property with semicolon-separated list (including wildcards) of Java classes
            // that can be used for routines based on Java static methods
            System.setProperty("hsqldb.method_class_names", "net.ucanaccess.converters.*");

        } catch (ClassNotFoundException ex) {
            LOGGER.log(Level.WARNING, "Unable to find hsqldb driver (version 2.x.x. or later) on your classpath");
            throw new UcanaccessRuntimeException(ex.getMessage());
        } catch (SQLException ex) {
            throw new UcanaccessRuntimeException(ex.getMessage());
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX) && url.length() > URL_PREFIX.length();
    }

    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        Map<String, String> unknownProps = new LinkedHashMap<>();
        Map<Property, String> knownProps = readProperties(props, url,
            (k, v) -> {
                unknownProps.put(k, v);
                LOGGER.log(Level.WARNING, "Unknown driver property {0} with value {1}", k, v);
            });

        int idxSemicolon = url.indexOf(';');
        String fileDbPath = idxSemicolon > 0 ? url.substring(URL_PREFIX.length(), idxSemicolon) : url.substring(URL_PREFIX.length());
        File fileDb = new File(fileDbPath);
        DBReferenceSingleton as = DBReferenceSingleton.getInstance();

        synchronized (UcanaccessDriver.class) {
            try {
                Session session = new Session();

                boolean alreadyLoaded = as.loaded(fileDb);
                FileFormat ff = null;
                if (knownProps.containsKey(newDatabaseVersion) && !fileDb.exists()) {
                    ff = FileFormat.parse(knownProps.get(newDatabaseVersion));
                }
                boolean useCustomOpener = knownProps.containsKey(jackcessOpener);

                IJackcessOpenerInterface jko = useCustomOpener
                    ? newJackcessOpenerInstance(knownProps.get(jackcessOpener))
                    : new DefaultJackcessOpener();

                Charset charsetArg = Try.catching(() -> Optional.ofNullable(knownProps.get(charset)).map(String::trim).map(Charset::forName).orElse(null))
                    .orThrow(ex -> new UcanaccessRuntimeException(MessageFormat.format("Unsupported charset in parameter {0}: {1}", charset, knownProps.get(charset)), ex));

                DBReference dbRef = alreadyLoaded ? as.getReference(fileDb) : as.loadReference(fileDb, ff, jko, knownProps.get(password), charsetArg);

                if (!alreadyLoaded) {
                    if ((useCustomOpener
                        || knownProps.containsKey(encrypt) && Boolean.parseBoolean(knownProps.get(encrypt)))
                        && (knownProps.containsKey(memory) && !Boolean.parseBoolean(knownProps.get(memory))
                            || knownProps.containsKey(keepMirror))) {
                        dbRef.setEncryptHSQLDB(true);
                    }

                    if (knownProps.containsKey(memory)) {
                        dbRef.setInMemory(Boolean.parseBoolean(knownProps.get(memory)));
                    }

                    if (knownProps.containsKey(lobScale)) {
                        Integer vl = validateLobScale(knownProps.get(lobScale));
                        dbRef.setLobScale(vl);
                    }

                    if (knownProps.containsKey(keepMirror)) {
                        dbRef.setInMemory(false);
                        if (dbRef.isEncryptHSQLDB()) {
                            LOGGER.log(Level.WARNING, "{0} parameter cannot be combined with parameters {1} or {2}, {3} skipped",
                                keepMirror, jackcessOpener, encrypt, keepMirror);
                        } else {
                            File dbMirror = new File(knownProps.get(keepMirror));
                            dbRef.setToKeepHsql(dbMirror);
                            if (knownProps.containsKey(readOnlyMirror)) {
                                dbRef.setMirrorReadOnly(Boolean.parseBoolean(knownProps.get(readOnlyMirror)));
                            }
                        }
                    }

                    if (knownProps.containsKey(showSchema)) {
                        dbRef.setShowSchema(Boolean.parseBoolean(knownProps.get(showSchema)));
                    }
                    if (knownProps.containsKey(inactivityTimeout)) {
                        int millis = 60000 * Integer.parseInt(knownProps.get(inactivityTimeout));
                        dbRef.setInactivityTimeout(millis);
                    }
                    if (knownProps.containsKey(singleConnection)) {
                        dbRef.setImmediatelyReleaseResources(Boolean.parseBoolean(knownProps.get(singleConnection)));
                    }
                    if (knownProps.containsKey(immediatelyReleaseResources)) {
                        dbRef.setImmediatelyReleaseResources(
                            Boolean.parseBoolean(knownProps.get(immediatelyReleaseResources)));
                    }
                    if (knownProps.containsKey(lockMdb)) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(knownProps.get(lockMdb)));
                    }

                    if (knownProps.containsKey(openExclusive)) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(knownProps.get(openExclusive)));
                    }

                    if (knownProps.containsKey(concatNulls)) {
                        dbRef.setConcatNulls(Boolean.parseBoolean(knownProps.get(concatNulls)));
                    }
                    if (knownProps.containsKey(preventReloading)) {
                        dbRef.setPreventReloading(Boolean.parseBoolean(knownProps.get(preventReloading)));
                    }
                    if (knownProps.containsKey(allowRemoteLinks)) {
                        dbRef.setAllowRemoteLinks(Boolean.parseBoolean(knownProps.get(allowRemoteLinks)));
                    }
                    if (knownProps.containsKey(reMap)) {
                        Map<String, String> map = Arrays.stream(knownProps.get(reMap).split("&")).map(s -> s.split("\\|")).filter(arr -> arr.length == 2)
                            .collect(Collectors.toMap(k1 -> k1[0], v1 -> v1[1], (v1, v2) -> v1, LinkedHashMap::new));
                        dbRef.setExternalResourcesMapping(map);
                    }
                    if (knownProps.containsKey(supportsAccessLike)) {
                        SQLConverter.setSupportsAccessLike(Boolean.parseBoolean(knownProps.get(supportsAccessLike)));
                    }
                    if (knownProps.containsKey(columnOrder)
                        && ColumnOrder.DISPLAY == ColumnOrder.parse(knownProps.get(columnOrder))) {
                        dbRef.setColumnOrderDisplay();
                    }
                    if (knownProps.containsKey(mirrorFolder) && dbRef.getToKeepHsql() == null) {
                        dbRef.setInMemory(false);
                        String fd = knownProps.get(mirrorFolder);
                        if ("java.io.tmpdir".equals(fd)) {
                            fd = System.getProperty("java.io.tmpdir");
                        }
                        dbRef.setMirrorFolder(new File(fd));
                    }
                    if (knownProps.containsKey(ignoreCase)) {
                        dbRef.setIgnoreCase(Boolean.parseBoolean(knownProps.get(ignoreCase)));
                    }

                    dbRef.getDbIO().setErrorHandler((cl, bt, location, ex) -> {
                        if (cl.getType().isTextual()) {
                            LOGGER.log(Level.WARNING, "Invalid textual value in table {0}, column {1}: it might look like {2}",
                                cl.getTable().getName(), cl.getName(), new String(bt));
                        }
                        throw new IOException(ex);
                    });
                }
                String pwd = dbRef.getDbIO().getDatabasePassword();
                if (pwd != null && !knownProps.containsKey(jackcessOpener)) {
                    if (!pwd.equals(knownProps.get(password))) {
                        throw new AuthenticationException();
                    }

                } else if (knownProps.containsKey(jackcessOpener)) {
                    String mpwd = knownProps.get(password);
                    session.setPassword(mpwd);
                }

                Optional.ofNullable(knownProps.get(user))
                    .ifPresent(session::setUser);

                SQLWarning sqlw = null;
                if (!alreadyLoaded) {
                    boolean toBeLoaded = !dbRef.loadedFromKeptMirror(session);

                    Connection conn = dbRef.getHSQLDBConnection(session);
                    // from version 2.7 hsqldb translates timestamps stored without timezone in the database
                    // into the default timezone. MS Access however does not know timezones, therefore assume timestamps are UTC
                    Try.withResources(conn::createStatement, st -> {
                        st.executeQuery("SET TIME ZONE 'UTC'");
                    }).orThrow();

                    LoadJet la = new LoadJet(conn, dbRef.getDbIO());
                    if (knownProps.containsKey(sysSchema)) {
                        boolean val = Boolean.parseBoolean(knownProps.get(sysSchema));
                        dbRef.setSysSchema(val);
                        la.setSysSchema(val);

                    }
                    if (knownProps.containsKey(skipIndexes)) {
                        boolean val = Boolean.parseBoolean(knownProps.get(skipIndexes));
                        dbRef.setSkipIndexes(val);
                        la.setSkipIndexes(val);
                    }

                    if (toBeLoaded) {
                        la.loadDB();
                    } else {
                        la.resetFunctionsDefault();
                    }
                    as.put(fileDb.getAbsolutePath(), dbRef);
                    sqlw = la.getLoadingWarnings();
                }

                Properties newProps = new Properties();
                knownProps.forEach((key, value) -> newProps.put(key.name(), value));
                newProps.putAll(unknownProps);

                UcanaccessConnection uc = new UcanaccessConnection(as.getReference(fileDb), newProps, session);
                uc.addWarnings(sqlw);
                uc.setUrl(url);
                return uc;
            } catch (Exception ex) {
                throw new UcanaccessSQLException(ex);
            }
        }
    }

    private Integer validateLobScale(String property) {
        try {
            int i = Integer.parseInt(property);

            if (i == 1 || i == 2 || i == 4 || i == 8 || i == 16 || i == 32) {
                return i;
            }
        } catch (Exception ignored) {
        }
        LOGGER.log(Level.WARNING, "Lobscale value must equal at least one of the following values: 1,2,4,8,16,32, skipping it");
        return null;
    }

    @Override
    public int getMajorVersion() {
        return VersionInfo.find(getClass()).getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return VersionInfo.find(getClass()).getMinorVersion();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties arg1) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * Loads the class with the given name and instantiates it via its public no-arg constructor,
     * provided it implements {@link IJackcessOpenerInterface}.
     * <p>
     * <strong>Security note:</strong> {@code className} must only ever originate from trusted,
     * static application configuration (see the {@code jackcessOpener} connection property).
     * It must never be built from untrusted or user-supplied input, since this method executes
     * the no-arg constructor of whatever class is named, and any class implementing
     * {@link IJackcessOpenerInterface} that is reachable on the classpath will pass the type
     * check regardless of what its constructor does.
     *
     * @param className fully qualified name of a class implementing {@link IJackcessOpenerInterface}
     * @return a new instance of the given class
     * @throws UcanaccessSQLException if the class cannot be loaded, does not implement
     *         {@link IJackcessOpenerInterface}, or cannot be instantiated via its no-arg constructor
     */
    private IJackcessOpenerInterface newJackcessOpenerInstance(String className) throws UcanaccessSQLException {
        Class<?> clazz = Try.catching(() -> Class.forName(className))
            .orThrow(ex -> new UcanaccessSQLException("Failed to load class " + className, ex));
        if (!IJackcessOpenerInterface.class.isAssignableFrom(clazz)) {
            throw new UcanaccessSQLException("Jackess Opener class must implement " + IJackcessOpenerInterface.class.getName());
        }
        Object instance = Try.catching(() -> clazz.getConstructor().newInstance())
            .orThrow(ex -> new UcanaccessSQLException("Failed to instantiate " + className, ex));
        return (IJackcessOpenerInterface) instance;
    }

    /**
     * Normalizes properties received as input and found in the driver url and returns a map of typed properties.<br>
     * The method differentiates between known and unknown properties by return known properties as typed properties
     * with their value and calling an optional consumer for each property key and value.<br>
     * If a property is found multiple times or found in both, the input properties as well as in the driver url,
     * the url has precedence over the input properties and the latest occurrence overall will be used.
     *
     * @param input input properties
     * @param url driver url
     * @param unknownConsumer consumer of unknown property key/value
     * @return map of unknown and normalized properties
     */
    static Map<Property, String> readProperties(Properties input, String url, BiConsumer<String, String> unknownConsumer) {
        Objects.requireNonNull(input, "Properties required");
        Objects.requireNonNull(url, "URL required");

        Map<Property, String> props = new EnumMap<>(Property.class);

        for (String key : input.stringPropertyNames()) {
            Property prop = parse(key);
            String val = input.getProperty(key);
            if (prop == null) {
                unknownConsumer.accept(key, val);
            } else {
                props.put(prop, val);
            }
        }

        Arrays.stream(url.split(";"))
            .skip(1)
            .map(s -> s.split("=")).forEach(arr -> {
                Property prop = parse(arr[0]);
                String val = arr.length > 1 ? arr[1].strip() : null;
                if (prop == null) {
                    unknownConsumer.accept(arr[0], val);
                } else {
                    props.put(prop, val);
                }
            });

        return props;
    }

}
