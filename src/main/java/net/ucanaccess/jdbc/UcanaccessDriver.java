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
import java.sql.*;
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

        } catch (ClassNotFoundException _ex) {
            LOGGER.log(Level.WARNING, "Unable to find hsqldb driver (version 2.x.x. or later) on your classpath");
            throw new UcanaccessRuntimeException(_ex.getMessage());
        } catch (SQLException _ex) {
            throw new UcanaccessRuntimeException(_ex.getMessage());
        }
    }

    @Override
    public boolean acceptsURL(String _url) {
        return _url != null && _url.startsWith(URL_PREFIX) && _url.length() > URL_PREFIX.length();
    }

    @Override
    public Connection connect(String _url, Properties _props) throws SQLException {
        if (!acceptsURL(_url)) {
            return null;
        }

        Map<String, String> unknownProps = new LinkedHashMap<>();
        Map<Property, String> props = readProperties(_props, _url,
            (k, v) -> {
                unknownProps.put(k, v);
                LOGGER.log(Level.WARNING, "Unknown driver property {0} with value {1}", k, v);
            });

        int idxSemicolon = _url.indexOf(';');
        String fileDbPath = idxSemicolon > 0 ? _url.substring(URL_PREFIX.length(), idxSemicolon) : _url.substring(URL_PREFIX.length());
        File fileDb = new File(fileDbPath);
        DBReferenceSingleton as = DBReferenceSingleton.getInstance();

        synchronized (UcanaccessDriver.class) {
            try {
                Session session = new Session();

                boolean alreadyLoaded = as.loaded(fileDb);
                FileFormat ff = null;
                if (props.containsKey(newDatabaseVersion) && !fileDb.exists()) {
                    ff = FileFormat.parse(props.get(newDatabaseVersion));
                }
                boolean useCustomOpener = props.containsKey(jackcessOpener);

                IJackcessOpenerInterface jko = useCustomOpener
                    ? newJackcessOpenerInstance(props.get(jackcessOpener))
                    : new DefaultJackcessOpener();
                DBReference dbRef = alreadyLoaded ? as.getReference(fileDb)
                    : as.loadReference(fileDb, ff, jko, props.get(password));

                if (!alreadyLoaded) {
                    if ((useCustomOpener
                        || props.containsKey(encrypt) && Boolean.parseBoolean(props.get(encrypt)))
                        && (props.containsKey(memory) && !Boolean.parseBoolean(props.get(memory))
                            || props.containsKey(keepMirror))) {
                        dbRef.setEncryptHSQLDB(true);
                    }

                    if (props.containsKey(memory)) {
                        dbRef.setInMemory(Boolean.parseBoolean(props.get(memory)));
                    }

                    if (props.containsKey(lobScale)) {
                        Integer vl = validateLobScale(props.get(lobScale));
                        dbRef.setLobScale(vl);
                    }

                    if (props.containsKey(keepMirror)) {
                        dbRef.setInMemory(false);
                        if (dbRef.isEncryptHSQLDB()) {
                            LOGGER.log(Level.WARNING, "{0} parameter cannot be combined with parameters {1} or {2}, {3} skipped",
                                keepMirror, jackcessOpener, encrypt, keepMirror);
                        } else {
                            File dbMirror = new File(props.get(keepMirror));
                            dbRef.setToKeepHsql(dbMirror);
                            if (props.containsKey(readOnlyMirror)) {
                                dbRef.setMirrorReadOnly(Boolean.parseBoolean(props.get(readOnlyMirror)));
                            }
                        }
                    }

                    if (props.containsKey(showSchema)) {
                        dbRef.setShowSchema(Boolean.parseBoolean(props.get(showSchema)));
                    }
                    if (props.containsKey(inactivityTimeout)) {
                        int millis = 60000 * Integer.parseInt(props.get(inactivityTimeout));
                        dbRef.setInactivityTimeout(millis);
                    }
                    if (props.containsKey(singleConnection)) {
                        dbRef.setImmediatelyReleaseResources(Boolean.parseBoolean(props.get(singleConnection)));
                    }
                    if (props.containsKey(immediatelyReleaseResources)) {
                        dbRef.setImmediatelyReleaseResources(
                            Boolean.parseBoolean(props.get(immediatelyReleaseResources)));
                    }
                    if (props.containsKey(lockMdb)) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(props.get(lockMdb)));
                    }

                    if (props.containsKey(openExclusive)) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(props.get(openExclusive)));
                    }

                    if (props.containsKey(concatNulls)) {
                        dbRef.setConcatNulls(Boolean.parseBoolean(props.get(concatNulls)));
                    }
                    if (props.containsKey(preventReloading)) {
                        dbRef.setPreventReloading(Boolean.parseBoolean(props.get(preventReloading)));
                    }
                    if (props.containsKey(reMap)) {
                        Map<String, String> map = Arrays.stream(props.get(reMap).split("&")).map(s -> s.split("\\|")).filter(arr -> arr.length == 2)
                            .collect(Collectors.toMap(k1 -> k1[0], v1 -> v1[1], (v1, v2) -> v1, LinkedHashMap::new));
                        dbRef.setExternalResourcesMapping(map);
                    }
                    if (props.containsKey(supportsAccessLike)) {
                        SQLConverter.setSupportsAccessLike(Boolean.parseBoolean(props.get(supportsAccessLike)));
                    }
                    if (props.containsKey(columnOrder)
                        && ColumnOrder.DISPLAY == ColumnOrder.parse(props.get(columnOrder))) {
                        dbRef.setColumnOrderDisplay();
                    }
                    if (props.containsKey(mirrorFolder) && dbRef.getToKeepHsql() == null) {
                        dbRef.setInMemory(false);
                        String fd = props.get(mirrorFolder);
                        if ("java.io.tmpdir".equals(fd)) {
                            fd = System.getProperty("java.io.tmpdir");
                        }
                        dbRef.setMirrorFolder(new File(fd));
                    }
                    if (props.containsKey(ignoreCase)) {
                        dbRef.setIgnoreCase(Boolean.parseBoolean(props.get(ignoreCase)));
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
                if (pwd != null && !props.containsKey(jackcessOpener)) {
                    if (!pwd.equals(props.get(password))) {
                        throw new AuthenticationException();
                    }

                } else if (props.containsKey(jackcessOpener)) {
                    String mpwd = props.get(password);
                    session.setPassword(mpwd);
                }

                Optional.ofNullable(props.get(user))
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
                    if (props.containsKey(sysSchema)) {
                        boolean val = Boolean.parseBoolean(props.get(sysSchema));
                        dbRef.setSysSchema(val);
                        la.setSysSchema(val);

                    }
                    if (props.containsKey(skipIndexes)) {
                        boolean val = Boolean.parseBoolean(props.get(skipIndexes));
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
                props.forEach((key, value) -> newProps.put(key.name(), value));
                newProps.putAll(unknownProps);

                UcanaccessConnection uc = new UcanaccessConnection(as.getReference(fileDb), newProps, session);
                uc.addWarnings(sqlw);
                uc.setUrl(_url);
                return uc;
            } catch (Exception _ex) {
                throw new UcanaccessSQLException(_ex);
            }
        }
    }

    private Integer validateLobScale(String _property) {
        try {
            int i = Integer.parseInt(_property);

            if (i == 1 || i == 2 || i == 4 || i == 8 || i == 16 || i == 32) {
                return i;
            }
        } catch (Exception _ignored) {
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

    private IJackcessOpenerInterface newJackcessOpenerInstance(String className) throws UcanaccessSQLException {
        Object instance = Try.catching(() -> Class.forName(className).getConstructor().newInstance()).orThrow(ex -> new UcanaccessSQLException("Failed to instantiate " + className, ex));

        if (instance instanceof IJackcessOpenerInterface) {
            return (IJackcessOpenerInterface) instance;
        }
        throw new UcanaccessSQLException("Jackess Opener class must implement " + IJackcessOpenerInterface.class.getName());
    }

    /**
     * Normalizes properties received as input and found in the driver url and returns a map of typed properties.<br>
     * The method differentiates between known and unknown properties by return known properties as typed properties
     * with their value and calling an optional consumer for each property key and value.<br>
     * If a property is found multiple times or found in both, the input properties as well as in the driver url,
     * the url has precedence over the input properties and the latest occurrence overall will be used.
     *
     * @param _input input properties
     * @param _url driver url
     * @param _unknownConsumer consumer of unknown property key/value
     * @return map of unknown and normalized properties
     */
    static Map<Property, String> readProperties(Properties _input, String _url, BiConsumer<String, String> _unknownConsumer) {
        Objects.requireNonNull(_input, "Properties required");
        Objects.requireNonNull(_url, "URL required");

        Map<Property, String> props = new EnumMap<>(Property.class);

        for (String key : _input.stringPropertyNames()) {
            Property prop = parse(key);
            String val = _input.getProperty(key);
            if (prop == null) {
                _unknownConsumer.accept(key, val);
            } else {
                props.put(prop, val);
            }
        }

        Arrays.stream(_url.split(";"))
            .skip(1)
            .map(s -> s.split("=")).forEach(arr -> {
                Property prop = parse(arr[0]);
                String val = arr.length > 1 ? arr[1].strip() : null;
                if (prop == null) {
                    _unknownConsumer.accept(arr[0], val);
                } else {
                    props.put(prop, val);
                }
            });

        return props;
    }

}
