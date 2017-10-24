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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import net.ucanaccess.util.Logger;

public class UcanaccessDataSource implements Serializable, Referenceable, DataSource {
    // private static final long serialVersionUID = 8574198937631043152L; // prior to v3.0.6
    private static final long serialVersionUID = -5622403376078548785L;

    private String                accessPath;
    private int                   loginTimeout = 0;
    private transient PrintWriter logWriter    = Logger.getLogPrintWriter();
    private String                password     = "";
    private String                user;

    private final Properties props = new Properties();

    public UcanaccessDataSource() {
    }

    /**
     * 
     * See {@link #setAccessPath(String)} for details.
     */
    public String getAccessPath() {
        return accessPath;
    }

    /**
     * See {@link #setColumnOrder(String)} for details.
     */
    public String getColumnOrder() {
        return getPropAsString("columnorder");
    }

    /**
     * See {@link #setConcatNulls(Boolean)} for details.
     */
    public Boolean getConcatNulls() {
        return getPropAsBoolean("concatnulls");
    }

    /**
     * Opens the connection to the Access database.
     * 
     * @return java.sql.Connection object
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    /**
     * Opens the connection to the Access database using the provided username and password.
     * 
     * @param username
     * @param password
     * @return java.sql.Connection object
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (username != null) {
            props.put("user", username);
        }
        if (password != null) {
            props.put("password", password);
        }
        return new UcanaccessDriver().connect(UcanaccessDriver.URL_PREFIX + accessPath, props);
    }

    /**
     * See {@link #setEncrypt(Boolean)} for details.
     */
    public Boolean getEncrypt() {
        return getPropAsBoolean("encrypt");
    }

    /**
     * See {@link #setIgnoreCase(Boolean)} for details.
     */
    public Boolean getIgnoreCase() {
        return getPropAsBoolean("ignorecase");
    }

    /**
     * See {@link #setImmediatelyReleaseResources(Boolean)} for details.
     */
    public Boolean getImmediatelyReleaseResources() {
        return getPropAsBoolean("immediatelyreleaseresources");
    }

    /**
     * See {@link #setInactivityTimeout(Integer)} for details.
     */
    public Integer getInactivityTimeout() {
        return getPropAsInteger("inactivitytimeout");
    }

    /**
     * See {@link #setJackcessOpener(String)} for details.
     */
    public String getJackcessOpener() {
        return getPropAsString("jackcessopener");
    }

    /**
     * See {@link #setKeepMirror(String)} for details.
     */
    public String getKeepMirror() {
        return getPropAsString("keepmirror");
    }

    /**
     * See {@link #setLobScale(Integer)} for details.
     */
    public Integer getLobScale() {
        return getPropAsInteger("lobscale");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.loginTimeout;
    }

    @Override
    public java.io.PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    /**
     * See {@link #setMemory(Boolean)} for details.
     */
    public Boolean getMemory() {
        return getPropAsBoolean("memory");
    }

    /**
     * See {@link #setMirrorFolder(String)} for details.
     */
    public String getMirrorFolder() {
        return getPropAsString("mirrorfolder");
    }

    /**
     * See {@link #setNewDatabaseVersion(String)} for details.
     */
    public String getNewDatabaseVersion() {
        return getPropAsString("newdatabaseversion");
    }

    /**
     * See {@link #setOpenExclusive(Boolean)} for details.
     */
    public Boolean getOpenExclusive() {
        return getPropAsBoolean("openexclusive");
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * See {@link #setPreventReloading(Boolean)} for details.
     */
    public Boolean getPreventReloading() {
        return getPropAsBoolean("preventreloading");
    }

    private Boolean getPropAsBoolean(String key) {
        Boolean rtn = null;
        String value = props.getProperty(key);
        if (value != null) {
            rtn = Boolean.valueOf(value);
        }
        return rtn;
    }

    private Integer getPropAsInteger(String key) {
        Integer rtn = null;
        String value = props.getProperty(key);
        if (value != null) {
            rtn = Integer.valueOf(value);
        }
        return rtn;
    }

    private String getPropAsString(String key) {
        return props.getProperty(key);
    }

    @Override
    public Reference getReference() throws NamingException {
        String clazz = UcanaccessDataSourceFactory.class.getName();
        Reference ref = new Reference(this.getClass().getName(), clazz, null);
        ref.add(new StringRefAddr("accessPath", this.getAccessPath()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));

        ref.add(new StringRefAddr("columnorder", this.getPropAsString("columnorder")));
        ref.add(new StringRefAddr("concatnulls", this.getPropAsString("concatnulls")));
        ref.add(new StringRefAddr("encrypt", this.getPropAsString("encrypt")));
        ref.add(new StringRefAddr("ignorecase", this.getPropAsString("ignorecase")));
        ref.add(new StringRefAddr("immediatelyreleaseresources", this.getPropAsString("immediatelyreleaseresources")));
        ref.add(new StringRefAddr("inactivitytimeout", this.getPropAsString("inactivitytimeout")));
        ref.add(new StringRefAddr("jackcessopener", this.getPropAsString("jackcessopener")));
        ref.add(new StringRefAddr("keepmirror", this.getPropAsString("keepmirror")));
        ref.add(new StringRefAddr("lobscale", this.getPropAsString("lobscale")));
        ref.add(new StringRefAddr("memory", this.getPropAsString("memory")));
        ref.add(new StringRefAddr("mirrorfolder", this.getPropAsString("mirrorfolder")));
        ref.add(new StringRefAddr("newdatabaseversion", this.getPropAsString("newdatabaseversion")));
        ref.add(new StringRefAddr("openexclusive", this.getPropAsString("openexclusive")));
        ref.add(new StringRefAddr("preventreloading", this.getPropAsString("preventreloading")));
        ref.add(new StringRefAddr("remap", this.getPropAsString("remap")));
        ref.add(new StringRefAddr("showschema", this.getPropAsString("showschema")));
        ref.add(new StringRefAddr("skipindexes", this.getPropAsString("skipindexes")));
        ref.add(new StringRefAddr("sysschema", this.getPropAsString("sysschema")));

        return ref;
    }

    /**
     * See {@link #setReMap(String)} for details.
     */
    public String getReMap() {
        return getPropAsString("remap");
    }

    /**
     * See {@link #setShowSchema(Boolean)} for details.
     */
    public Boolean getShowSchema() {
        return getPropAsBoolean("showschema");
    }

    /**
     * See {@link #setSkipIndexes(Boolean)} for details.
     */
    public Boolean getSkipIndexes() {
        return getPropAsBoolean("skipindexes");
    }

    /**
     * See {@link #setSysSchema(Boolean)} for details.
     */
    public Boolean getSysSchema() {
        return getPropAsBoolean("sysschema");
    }

    public String getUser() {
        return user;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Specifies the path to the Access database file, for example,
     * <p>
     * C:/folder/database.accdb
     *
     * @param accessPath
     */
    public void setAccessPath(String accessPath) {
        this.accessPath = accessPath;
    }

    /**
     * Controls the order in which columns are returned by operations like SELECT *. The default is "DATA". See the
     * UCanAccess website for details.
     *
     * @param value
     *            Valid values for this parameter are: "DATA", or "DISPLAY".
     * @since 2.0.9
     */
    public void setColumnOrder(String value) {
        setProp("columnorder", value, new ArrayList<Object>(Arrays.asList("DATA", "DISPLAY")));
    }

    /**
     * Controls what is returned by concatenations involving null values. This setting affects <i>all</i> concatenation
     * operators (&amp;, +, ||).
     *
     * @param value
     *            <ul>
     *            <li><b>false</b> (the default): null &amp; 'thing' returns 'thing'
     *            <li><b>true</b>: null &amp; 'thing' returns null
     *            </ul>
     * @since 3.0.0
     */
    public void setConcatNulls(Boolean value) {
        setProp("concatnulls", value, null);
    }

    /**
     * Encrypts the backing database.
     *
     * @param value
     *            Setting this value to <b>true</b> tells UCanAccess to encrypt the HSQLDB backing database.
     * @since 1.0.4
     */
    public void setEncrypt(Boolean value) {
        setProp("encrypt", value, null);
    }

    /**
     * Enables case-insensitive string comparisons.
     *
     * @param value
     *            (default = true)
     */
    public void setIgnoreCase(Boolean value) {
        setProp("ignorecase", value, null);
    }

    /**
     * Releases the Access database and shuts down HSQLDB immediately after the Connection is closed.
     *
     * @param value
     *            Setting this value to <b>true</b> tells UCanAccess to close the Access database file, shut down
     *            HSQLDB, and release all related resources (e.g., memory or disk space) as soon as the Connection is
     *            closed. Otherwise, UCanAccess will "hold on" to those resources for the
     *            {@link #setInactivityTimeout(Integer)} in case the Access database gets re-opened.
     */
    public void setImmediatelyReleaseResources(Boolean value) {
        setProp("immediatelyreleaseresources", value, null);
    }

    /**
     * For an in-memory backing database (memory=true), this parameter controls the time (in minutes, default=2), that
     * HSQLDB remains active and the in-memory database remains available after the last connection is closed.
     * 
     * @param value
     *            (in minutes)
     * @since 0.1.1
     */
    public void setInactivityTimeout(Integer value) {
        setProp("inactivitytimeout", value, null);
    }

    /**
     * Specifies the name of the custom class to be used by Jackcess when opening the Access database. Mainly for use
     * with Jackcess Encrypt to open encrypted databases. See the UCanAccess website for details.
     *
     * @param value
     * @since 0.0.2
     */
    public void setJackcessOpener(String value) {
        setProp("jackcessopener", value, null);
    }

    /**
     * Specifies the path where a persistent copy of the HSQLDB backing database files should be stored. Implies
     * memory=false. See the UCanAccess website for details.
     *
     * @param value
     * @since 2.0.2
     */
    public void setKeepMirror(String value) {
        setProp("keepmirror", value, null);
    }

    /**
     * Controls the allocation unit size for "OLE Object" (i.e., BLOB) data in the HSQLDB backing database. See the
     * UCanAccess website for details.
     * 
     * @param value
     *            Values allowed are 1, 2, 4, 8, 16, or 32 (the unit is kB). Default is 2 if memory=true, and 32 (the
     *            HSQLDB default) otherwise.
     * @since 2.0.9.4
     */
    public void setLobScale(Integer value) {
        setProp("lobscale", value, new ArrayList<Object>(Arrays.asList(1, 2, 4, 8, 16, 32)));
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    @Override
    public void setLogWriter(PrintWriter logWriter) throws SQLException {
        Logger.setLogPrintWriter(logWriter);
    }

    /**
     * Controls whether the HSQLDB backing database is created in memory (the default) or on disk.
     *
     * @param value
     *            Create the backing database in memory (value=true, the default) or on disk (value=false). Creating the
     *            backing database on disk will be slower, but it can greatly reduce JVM memory usage.
     */
    public void setMemory(Boolean value) {
        setProp("memory", value, null);
    }

    /**
     * Specifies the path where the temporary HSQLDB backing database files should be stored. Implies memory=false. To
     * create a persistent copy of the backing database, use {@link #setKeepMirror(String)} instead. See the UCanAccess
     * website for details.
     *
     * @param value
     * @since 2.0.9.3
     */
    public void setMirrorFolder(String value) {
        setProp("mirrorfolder", value, null);
    }

    /**
     * Creates a new, empty Access database in this format if the database specified by {@link #setAccessPath(String)}
     * does not exist.
     *
     * @param value
     *            Valid values for this parameter are: "V2000", "V2003", "V2007", or "V2010".
     */
    public void setNewDatabaseVersion(String value) {
        setProp("newdatabaseversion", value, new ArrayList<Object>(Arrays.asList("V2000", "V2003", "V2007", "V2010")));
    }

    /**
     * Opens the Access database in "exclusive" mode.
     *
     * @param value
     *            Setting this value to <b>true</b> tells UCanAccess to open the Access database as "exclusive"
     *            (preventing other processes from opening it at the same time).
     */
    public void setOpenExclusive(Boolean value) {
        setProp("openexclusive", value, null);
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Prevents unnecessary re-loading of backing database under very specific circumstances. USE WITH CAUTION! See the
     * UCanAccess website for details.
     *
     * @param value
     *            Setting this value to <b>true</b> prevents UCanAccess from unnecessarily re-loading the backing
     *            database under very specific circumstances.
     * @since 3.0.0
     */
    public void setPreventReloading(Boolean value) {
        setProp("preventreloading", value, null);
    }

    private void setProp(String key, Object value, ArrayList<Object> validValues) {
        if (value == null) {
            props.remove(key);
        } else {
            boolean isValid = true;
            if (validValues != null) {
                if (value instanceof String) {
                    isValid = validValues.contains(value.toString().toUpperCase());
                } else {
                    isValid = validValues.contains(value);
                }
            }
            if (isValid) {
                props.setProperty(key, value.toString());
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    /**
     * For this connection, temporarily re-directs linked tables in the Access database to point to a different Access
     * database. See the UCanAccess website for details.
     *
     * @param value
     * @since 2.0.2
     */
    public void setReMap(String value) {
        setProp("remap", value, null);
    }

    /**
     * Exposes the HSQLDB catalog and schema names (e.g., "PUBLIC") in DatabaseMetadata.
     *
     * @param value
     *            (default = false)
     */
    public void setShowSchema(Boolean value) {
        setProp("showschema", value, null);
    }

    /**
     * Reduces memory consumption by not creating simple indexes in the backing database.
     *
     * @param value
     *            Setting this value to <b>true</b> tells UCanAccess to skip the creation of simple indexes (not
     *            associated with a constraint). It doesn't have an effect on referential integrity constraints (i.e.,
     *            Index Unique, Foreign Key or Primary Key).
     * @since 2.0.9.4
     */
    public void setSkipIndexes(Boolean value) {
        setProp("skipindexes", value, null);
    }

    /**
     * Exposes the Access system tables in a read-only schema named "SYS".
     *
     * @param value
     *            (default = false)
     */
    public void setSysSchema(Boolean value) {
        setProp("sysschema", value, null);
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
