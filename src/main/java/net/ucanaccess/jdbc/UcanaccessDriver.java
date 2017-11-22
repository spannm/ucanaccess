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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.util.ErrorHandler;

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

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return (url.startsWith(URL_PREFIX) && url.length() > URL_PREFIX.length());
    }

    @Override
    public Connection connect(String _url, Properties _props) throws SQLException {
        if (!this.acceptsURL(_url)) {
            return null;
        }
        readProperties(_props, _url);
        String fileMdbPath = _url.indexOf(";") > 0 ? _url.substring(URL_PREFIX.length(), _url.indexOf(";"))
                : _url.substring(URL_PREFIX.length());
        File mdb = new File(fileMdbPath);
        DBReferenceSingleton as = DBReferenceSingleton.getInstance();

        synchronized (UcanaccessDriver.class) {
            try {
                Session session = new Session();

                boolean alreadyLoaded = as.loaded(mdb);
                FileFormat ff = null;
                if (_props.containsKey("newdatabaseversion")) {
                    if (!mdb.exists()) {
                        ff = FileFormat.valueOf(_props.getProperty("newdatabaseversion").toUpperCase());
                    }

                }
                boolean useCustomOpener = _props.containsKey("jackcessopener");

                JackcessOpenerInterface jko = useCustomOpener
                        ? newJackcessOpenerInstance(_props.getProperty("jackcessopener")) : new DefaultJackcessOpener();
                DBReference dbRef = alreadyLoaded ? as.getReference(mdb)
                        : as.loadReference(mdb, ff, jko, _props.getProperty("password"));

                if (!alreadyLoaded) {
                    if ((useCustomOpener
                            || (_props.containsKey("encrypt") && Boolean.parseBoolean(_props.getProperty("encrypt"))))
                            && ((_props.containsKey("memory") && !Boolean.parseBoolean(_props.getProperty("memory")))
                                    || _props.containsKey("keepmirror"))) {
                        dbRef.setEncryptHSQLDB(true);
                    }

                    if (_props.containsKey("memory")) {
                        dbRef.setInMemory(Boolean.parseBoolean(_props.getProperty("memory")));
                    }

                    if (_props.containsKey("lobscale")) {
                        Integer vl = validateLobScale(_props.getProperty("lobscale"));
                        dbRef.setLobScale(vl);
                    }

                    if (_props.containsKey("keepmirror")) {
                        dbRef.setInMemory(false);
                        if (dbRef.isEncryptHSQLDB()) {
                            Logger.logWarning(Messages.KEEP_MIRROR_AND_OTHERS);
                        } else {
                            File dbMirror =
                                    new File(_props.getProperty("keepmirror") + mdb.getName().toUpperCase().hashCode());
                            dbRef.setToKeepHsql(dbMirror);
                            if (_props.containsKey("readonlymirror")) {
                                dbRef.setMirrorReadOnly(Boolean.parseBoolean(_props.getProperty("readonlymirror")));
                            }
                        }
                    }

                    if (_props.containsKey("showschema")) {
                        dbRef.setShowSchema(Boolean.parseBoolean(_props.getProperty("showschema")));
                    }
                    if (_props.containsKey("inactivitytimeout")) {
                        int millis = 60000 * Integer.parseInt(_props.getProperty("inactivitytimeout"));
                        dbRef.setInactivityTimeout(millis);
                    }
                    if (_props.containsKey("singleconnection")) {
                        dbRef.setImmediatelyReleaseResources(Boolean.parseBoolean(_props.getProperty("singleconnection")));
                    }
                    if (_props.containsKey("immediatelyreleaseresources")) {
                        dbRef.setImmediatelyReleaseResources(
                                Boolean.parseBoolean(_props.getProperty("immediatelyreleaseresources")));
                    }
                    if (_props.containsKey("lockmdb")) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(_props.getProperty("lockmdb")));
                    }

                    if (_props.containsKey("openexclusive")) {
                        dbRef.setOpenExclusive(Boolean.parseBoolean(_props.getProperty("openexclusive")));
                    }

                    if (_props.containsKey("concatnulls")) {
                        dbRef.setConcatNulls(Boolean.parseBoolean(_props.getProperty("concatnulls")));
                    }
                    if (_props.containsKey("preventreloading")) {
                        dbRef.setPreventReloading(Boolean.parseBoolean(_props.getProperty("preventreloading")));
                    }
                    if (_props.containsKey("remap")) {
                        dbRef.setExternalResourcesMapping(toMap(_props.getProperty("remap")));
                    }
                    if (_props.containsKey("supportsaccesslike")) {
                        SQLConverter
                                .setSupportsAccessLike(Boolean.parseBoolean(_props.getProperty("supportsaccesslike")));
                    }
                    if (_props.containsKey("columnorder") && "display".equalsIgnoreCase(_props.getProperty("columnorder"))) {
                        dbRef.setColumnOrderDisplay();
                    }
                    if (_props.containsKey("mirrorfolder") && dbRef.getToKeepHsql() == null) {
                        dbRef.setInMemory(false);
                        String fd = _props.getProperty("mirrorfolder");
                        dbRef.setMirrorFolder(
                                new File("java.io.tmpdir".equals(fd) ? System.getProperty("java.io.tmpdir") : fd));
                    }
                    if (_props.containsKey("ignorecase")) {
                        dbRef.setIgnoreCase(Boolean.parseBoolean(_props.getProperty("ignorecase")));
                    }

                    dbRef.getDbIO().setErrorHandler(new ErrorHandler() {
                        @Override
                        public Object handleRowError(Column cl, byte[] bt, Location location, Exception ex)
                                throws IOException {
                            if (cl.getType().isTextual()) {
                                Logger.logParametricWarning(Messages.INVALID_CHARACTER_SEQUENCE,
                                        cl.getTable().getName(), cl.getName(), new String(bt));
                            }
                            throw new IOException(ex.getMessage());
                        }
                    });
                }
                String pwd = dbRef.getDbIO().getDatabasePassword();
                if (pwd != null && !_props.containsKey("jackcessopener")) {
                    if (!pwd.equals(_props.get("password"))) {
                        throw new UcanaccessSQLException(ExceptionMessages.NOT_A_VALID_PASSWORD);
                    }

                } else if (_props.containsKey("jackcessopener")) {
                    String mpwd = _props.getProperty("password");
                    session.setPassword(mpwd);
                }

                String user = _props.getProperty("user");
                if (user != null) {
                    session.setUser(user);
                }

                SQLWarning sqlw = null;
                if (!alreadyLoaded) {
                    boolean toBeLoaded = !dbRef.loadedFromKeptMirror(session);
                    LoadJet la = new LoadJet(dbRef.getHSQLDBConnection(session), dbRef.getDbIO());
                    Logger.turnOffJackcessLog();
                    if (_props.containsKey("sysschema")) {
                        boolean sysSchema = Boolean.parseBoolean(_props.getProperty("sysschema"));
                        dbRef.setSysSchema(sysSchema);
                        la.setSysSchema(sysSchema);

                    }
                    if (_props.containsKey("skipindexes")) {
                        boolean skipIndexes = Boolean.parseBoolean(_props.getProperty("skipindexes"));
                        dbRef.setSkipIndexes(skipIndexes);
                        la.setSkipIndexes(skipIndexes);
                    }

                    if (toBeLoaded) {
                        la.loadDB();
                    } else {
                        la.resetFunctionsDefault();
                    }
                    as.put(mdb.getAbsolutePath(), dbRef);
                    sqlw = la.getLoadingWarnings();
                }

                UcanaccessConnection uc = new UcanaccessConnection(as.getReference(mdb), _props, session);
                uc.addWarnings(sqlw);
                uc.setUrl(_url);
                return uc;
            } catch (Exception e) {
                throw new UcanaccessSQLException(e);
            }
        }
    }

    private Integer validateLobScale(String property) {
        try {
            Integer i = Integer.parseInt(property);

            if (i == 1 || i == 2 || i == 4 || i == 8 || i == 16 || i == 32) {
                return i;
            }

        } catch (Exception e) {

        }
        Logger.logWarning(Logger.Messages.LOBSCALE);
        return null;
    }

    private Map<String, String> toMap(String property) {
        Map<String, String> hm = new HashMap<String, String>();
        StringTokenizer st = new StringTokenizer(property, "&");
        while (st.hasMoreTokens()) {
            String entry = st.nextToken();
            if (entry.indexOf("|") < 0) {
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

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties arg1) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    private JackcessOpenerInterface newJackcessOpenerInstance(String className)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, UcanaccessSQLException {
        Object newInstance = Class.forName(className).newInstance();
        if (!(newInstance instanceof JackcessOpenerInterface)) {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_JACKCESS_OPENER);
        }
        return (JackcessOpenerInterface) newInstance;
    }

    private void readProperties(Properties pr, String url) {
        Properties nb = new Properties();

        for (Entry<Object, Object> entry : pr.entrySet()) {
            String key = (String) entry.getKey();
            if (key != null) {
                nb.put(key.toLowerCase(), entry.getValue());
            }
        }
        pr.clear();
        pr.putAll(nb);
        StringTokenizer st = new StringTokenizer(url, ";");
        while (st.hasMoreTokens()) {
            String entry = st.nextToken();
            int sep = entry.indexOf("=");
            if (sep > 0 && entry.length() > sep) {
                pr.put(entry.substring(0, sep).toLowerCase(), entry.substring(sep + 1, entry.length()));
            }
        }
    }
}
