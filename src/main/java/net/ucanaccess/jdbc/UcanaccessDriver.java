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
    public Connection connect(String url, Properties pr) throws SQLException {

        if (!this.acceptsURL(url)) {
            return null;
        }
        readProperties(pr, url);
        String fileMdbPath = url.indexOf(";") > 0 ? url.substring(URL_PREFIX.length(), url.indexOf(";"))
                : url.substring(URL_PREFIX.length());
        File mdb = new File(fileMdbPath);
        DBReferenceSingleton as = DBReferenceSingleton.getInstance();

        synchronized (UcanaccessDriver.class) {
            try {
                Session session = new Session();

                boolean alreadyLoaded = as.loaded(mdb);
                FileFormat ff = null;
                if (pr.containsKey("newdatabaseversion")) {
                    if (!mdb.exists()) {

                        ff = FileFormat.valueOf(pr.getProperty("newdatabaseversion").toUpperCase());
                    }

                }
                boolean useCustomOpener = pr.containsKey("jackcessopener");

                JackcessOpenerInterface jko = useCustomOpener
                        ? newJackcessOpenerInstance(pr.getProperty("jackcessopener")) : new DefaultJackcessOpener();
                DBReference ref = alreadyLoaded ? as.getReference(mdb)
                        : as.loadReference(mdb, ff, jko, pr.getProperty("password"));

                if (!alreadyLoaded) {
                    if ((useCustomOpener
                            || (pr.containsKey("encrypt") && "true".equalsIgnoreCase(pr.getProperty("encrypt"))))
                            && ((pr.containsKey("memory") && !"true".equalsIgnoreCase(pr.getProperty("memory")))
                                    || pr.containsKey("keepmirror"))) {
                        ref.setEncryptHSQLDB(true);
                    }

                    if (pr.containsKey("memory")) {
                        ref.setInMemory("true".equalsIgnoreCase(pr.getProperty("memory")));
                    }

                    if (pr.containsKey("lobscale")) {
                        Integer vl = validateLobScale(pr.getProperty("lobscale"));
                        ref.setLobScale(vl);
                    }

                    if (pr.containsKey("keepmirror")) {
                        ref.setInMemory(false);
                        if (ref.isEncryptHSQLDB()) {
                            Logger.logWarning(Messages.KEEP_MIRROR_AND_OTHERS);
                        } else {
                            File dbMirror =
                                    new File(pr.getProperty("keepmirror") + mdb.getName().toUpperCase().hashCode());
                            ref.setToKeepHsql(dbMirror);
                            if (pr.containsKey("readonlymirror")) {
                                ref.setMirrorReadOnly("true".equalsIgnoreCase(pr.getProperty("readonlymirror")));
                            }
                        }
                    }

                    if (pr.containsKey("showschema")) {
                        ref.setShowSchema("true".equalsIgnoreCase(pr.getProperty("showschema")));
                    }

                    if (pr.containsKey("inactivitytimeout")) {
                        int millis = 60000 * Integer.parseInt(pr.getProperty("inactivitytimeout"));
                        ref.setInactivityTimeout(millis);
                    }

                    if (pr.containsKey("singleconnection")) {
                        ref.setImmediatelyReleaseResources("true".equalsIgnoreCase(pr.getProperty("singleconnection")));
                    }

                    if (pr.containsKey("immediatelyreleaseresources")) {
                        ref.setImmediatelyReleaseResources(
                                "true".equalsIgnoreCase(pr.getProperty("immediatelyreleaseresources")));
                    }

                    if (pr.containsKey("lockmdb")) {
                        ref.setOpenExclusive("true".equalsIgnoreCase(pr.getProperty("lockmdb")));
                    }

                    if (pr.containsKey("openexclusive")) {
                        ref.setOpenExclusive("true".equalsIgnoreCase(pr.getProperty("openexclusive")));
                    }

                    if (pr.containsKey("concatnulls")) {
                        ref.setConcatNulls("true".equalsIgnoreCase(pr.getProperty("concatnulls")));
                    }
                    if (pr.containsKey("preventreloading")) {
                        ref.setPreventReloading("true".equalsIgnoreCase(pr.getProperty("preventreloading")));
                    }

                    if (pr.containsKey("remap")) {
                        ref.setExternalResourcesMapping(toMap(pr.getProperty("remap")));
                    }

                    if (pr.containsKey("supportsaccesslike")) {
                        SQLConverter
                                .setSupportsAccessLike("true".equalsIgnoreCase(pr.getProperty("supportsaccesslike")));
                    }

                    if (pr.containsKey("columnorder") && "display".equalsIgnoreCase(pr.getProperty("columnorder"))) {
                        ref.setColumnOrderDisplay();
                    }

                    if (pr.containsKey("mirrorfolder") && ref.getToKeepHsql() == null) {
                        ref.setInMemory(false);
                        String fd = pr.getProperty("mirrorfolder");
                        ref.setMirrorFolder(
                                new File("java.io.tmpdir".equals(fd) ? System.getProperty("java.io.tmpdir") : fd));
                    }
                    if (pr.containsKey("ignorecase")) {
                        ref.setIgnoreCase("true".equalsIgnoreCase(pr.getProperty("ignorecase")));
                    }

                    ref.getDbIO().setErrorHandler(new ErrorHandler() {
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
                String pwd = ref.getDbIO().getDatabasePassword();
                if (pwd != null && !pr.containsKey("jackcessopener")) {
                    if (!pwd.equals(pr.get("password"))) {
                        throw new UcanaccessSQLException(ExceptionMessages.NOT_A_VALID_PASSWORD);
                    }

                } else if (pr.containsKey("jackcessopener")) {
                    String mpwd = pr.getProperty("password");
                    session.setPassword(mpwd);
                }

                String user = pr.getProperty("user");
                if (user != null) {
                    session.setUser(user);
                }

                SQLWarning sqlw = null;
                if (!alreadyLoaded) {
                    boolean toBeLoaded = !ref.loadedFromKeptMirror(session);
                    LoadJet la = new LoadJet(ref.getHSQLDBConnection(session), ref.getDbIO());
                    Logger.turnOffJackcessLog();
                    if (pr.containsKey("sysschema")) {
                        boolean sysSchema = "true".equalsIgnoreCase(pr.getProperty("sysschema"));
                        ref.setSysSchema(sysSchema);
                        la.setSysSchema(sysSchema);

                    }
                    if (pr.containsKey("skipindexes")) {
                        boolean skipIndexes = "true".equalsIgnoreCase(pr.getProperty("skipindexes"));
                        ref.setSkipIndexes(skipIndexes);
                        la.setSkipIndexes(skipIndexes);
                    }

                    if (toBeLoaded) {
                        la.loadDB();
                    } else {
                        la.resetFunctionsDefault();
                    }
                    as.put(mdb.getAbsolutePath(), ref);
                    sqlw = la.getLoadingWarnings();
                }

                UcanaccessConnection uc = new UcanaccessConnection(as.getReference(mdb), pr, session);
                uc.addWarnings(sqlw);
                uc.setUrl(url);
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
            int sep;
            if ((sep = entry.indexOf("=")) > 0 && entry.length() > sep) {
                pr.put(entry.substring(0, sep).toLowerCase(), entry.substring(sep + 1, entry.length()));
            }
        }
    }
}
