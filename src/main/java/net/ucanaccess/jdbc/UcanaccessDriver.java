package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;

import com.healthmarketscience.jackcess.Database.FileFormat;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.log.Logger;
import net.ucanaccess.log.LoggerMessageEnum;
import net.ucanaccess.type.ColumnOrder;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.function.BiConsumer;

public final class UcanaccessDriver implements Driver {

    public static final String URL_PREFIX = "jdbc:ucanaccess://";

    static {
        try {
            DriverManager.registerDriver(new UcanaccessDriver());
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            // Set property with semicolon-separated list (including wildcards) of Java classes
            // that can be used for routines based on Java static methods
            System.setProperty("hsqldb.method_class_names", "net.ucanaccess.converters.*");

        } catch (ClassNotFoundException _ex) {
            Logger.logWarning(LoggerMessageEnum.HSQLDB_DRIVER_NOT_FOUND);
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
                Logger.logWarning(LoggerMessageEnum.UNKNOWN_DRIVER_PROPERTY, k, v);
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
                if (props.containsKey(newDatabaseVersion)) {
                    if (!fileDb.exists()) {
                        ff = FileFormat.valueOf(props.get(newDatabaseVersion).toUpperCase());
                    }

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
                            Logger.logWarning(LoggerMessageEnum.KEEP_MIRROR_AND_OTHERS);
                        } else {
                            File dbMirror =
                                new File(props.get(keepMirror) + fileDb.getName().toUpperCase().hashCode());
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
                        dbRef.setExternalResourcesMapping(toMap(props.get(reMap)));
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
                            Logger.logWarning(LoggerMessageEnum.INVALID_CHARACTER_SEQUENCE,
                                cl.getTable().getName(), cl.getName(), new String(bt));
                        }
                        throw new IOException(ex.getMessage());
                    });
                }
                String pwd = dbRef.getDbIO().getDatabasePassword();
                if (pwd != null && !props.containsKey(jackcessOpener)) {
                    if (!pwd.equals(props.get(password))) {
                        throw new UcanaccessSQLException(ExceptionMessages.NOT_A_VALID_PASSWORD);
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
                    Logger.turnOffJackcessLog();
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
                props.entrySet().stream()
                    .forEach(e -> newProps.put(e.getKey().name(), e.getValue()));
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
            Integer i = Integer.parseInt(_property);

            if (i == 1 || i == 2 || i == 4 || i == 8 || i == 16 || i == 32) {
                return i;
            }

        } catch (Exception _ignored) {

        }
        Logger.logWarning(LoggerMessageEnum.LOBSCALE);
        return null;
    }

    private Map<String, String> toMap(String property) {
        Map<String, String> hm = new HashMap<>();
        StringTokenizer st = new StringTokenizer(property, "&");
        while (st.hasMoreTokens()) {
            String entry = st.nextToken();
            if (!entry.contains("|")) {
                continue;
            }
            hm.put(entry.substring(0, entry.indexOf('|')).toLowerCase(), entry.substring(entry.indexOf('|') + 1));
        }
        return hm;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
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

    private IJackcessOpenerInterface newJackcessOpenerInstance(String className)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, UcanaccessSQLException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Object newInstance = Class.forName(className).getConstructor().newInstance();

        if (!IJackcessOpenerInterface.class.isInstance(newInstance)) {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_JACKCESS_OPENER);
        }
        return (IJackcessOpenerInterface) newInstance;
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
