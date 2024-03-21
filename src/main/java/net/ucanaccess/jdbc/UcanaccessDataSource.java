package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.type.AccessVersion;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

public class UcanaccessDataSource implements Serializable, Referenceable, DataSource {
    private static final long           serialVersionUID = -5622403376078548785L;

    private String                      accessPath;
    private int                         loginTimeout     = 0;
    private transient PrintWriter       logWriter        = new PrintWriter(System.out);
    private final Map<Property, String> userPass         = new EnumMap<>(Property.class);

    private final Map<Property, String> props            = new EnumMap<>(Property.class);

    public UcanaccessDataSource() {
    }

    /**
     *
     * See {@link #setAccessPath(String)} for details.
     */
    public String getAccessPath() {
        return accessPath;
    }

    public String getUser() {
        return userPass.get(user);
    }

    /**
     * See {@link #setColumnOrder(String)} for details.
     */
    public String getColumnOrder() {
        return getPropAsString(columnOrder);
    }

    /**
     * See {@link #setConcatNulls(Boolean)} for details.
     */
    public Boolean getConcatNulls() {
        return getPropAsBoolean(concatNulls);
    }

    /**
     * Opens the connection to the Access database.
     *
     * @return java.sql.Connection object
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(userPass.get(user), userPass.get(password));
    }

    /**
     * Opens the connection to the Access database using the provided user name and password.
     *
     * @param _user user name
     * @param _pass password
     * @return database connection
     */
    @Override
    public Connection getConnection(String _user, String _pass) throws SQLException {
        // do not store user/pass in member props
        Properties copy = props.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().name(), Entry::getValue, (v1, v2) -> v1, Properties::new));
        Optional.ofNullable(_user).ifPresent(u -> copy.put(user.name(), u));
        Optional.ofNullable(_pass).ifPresent(p -> copy.put(password.name(), p));
        return new UcanaccessDriver().connect(UcanaccessDriver.URL_PREFIX + accessPath, copy);
    }

    /**
     * See {@link #setEncrypt(Boolean)} for details.
     */
    public Boolean getEncrypt() {
        return getPropAsBoolean(encrypt);
    }

    /**
     * See {@link #setIgnoreCase(Boolean)} for details.
     */
    public Boolean getIgnoreCase() {
        return getPropAsBoolean(ignoreCase);
    }

    /**
     * See {@link #setImmediatelyReleaseResources(Boolean)} for details.
     */
    public Boolean getImmediatelyReleaseResources() {
        return getPropAsBoolean(immediatelyReleaseResources);
    }

    /**
     * See {@link #setInactivityTimeout(Integer)} for details.
     */
    public Integer getInactivityTimeout() {
        return getPropAsInteger(inactivityTimeout);
    }

    /**
     * See {@link #setJackcessOpener(String)} for details.
     */
    public String getJackcessOpener() {
        return getPropAsString(jackcessOpener);
    }

    /**
     * See {@link #setKeepMirror(String)} for details.
     */
    public String getKeepMirror() {
        return getPropAsString(keepMirror);
    }

    /**
     * See {@link #setLobScale(Integer)} for details.
     */
    public Integer getLobScale() {
        return getPropAsInteger(lobScale);
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * See {@link #setMemory(Boolean)} for details.
     */
    public Boolean getMemory() {
        return getPropAsBoolean(memory);
    }

    /**
     * See {@link #setMirrorFolder(String)} for details.
     */
    public String getMirrorFolder() {
        return getPropAsString(mirrorFolder);
    }

    /**
     * See {@link #setNewDatabaseVersion(String)} for details.
     */
    public String getNewDatabaseVersion() {
        return getPropAsString(newDatabaseVersion);
    }

    /**
     * See {@link #setOpenExclusive(Boolean)} for details.
     */
    public Boolean getOpenExclusive() {
        return getPropAsBoolean(openExclusive);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * See {@link #setPreventReloading(Boolean)} for details.
     */
    public Boolean getPreventReloading() {
        return getPropAsBoolean(preventReloading);
    }

    private Boolean getPropAsBoolean(Metadata.Property _key) {
        return Optional.ofNullable(getPropAsString(_key))
            .map(Boolean::valueOf)
            .orElse(null);
    }

    private Integer getPropAsInteger(Metadata.Property _key) {
        return Optional.ofNullable(getPropAsString(_key))
            .map(Integer::valueOf)
            .orElse(null);
    }

    private String getPropAsString(Metadata.Property _key) {
        return _key == null ? null : props.get(_key);
    }

    @Override
    public Reference getReference() {
        String clazz = UcanaccessDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), clazz, null);
        ref.add(new StringRefAddr("accessPath", getAccessPath()));
        ref.add(new StringRefAddr(user.name(), userPass.get(user)));
        ref.add(new StringRefAddr(password.name(), userPass.get(password)));

        Arrays.stream(values())
            .filter(e -> e != user)
            .filter(e -> e != password)
            .forEach(e -> ref.add(new StringRefAddr(e.name(), getPropAsString(e))));

        return ref;
    }

    /**
     * See {@link #setReMap(String)} for details.
     */
    public String getReMap() {
        return getPropAsString(reMap);
    }

    /**
     * See {@link #setShowSchema(Boolean)} for details.
     */
    public Boolean getShowSchema() {
        return getPropAsBoolean(showSchema);
    }

    /**
     * See {@link #setSkipIndexes(Boolean)} for details.
     */
    public Boolean getSkipIndexes() {
        return getPropAsBoolean(skipIndexes);
    }

    /**
     * See {@link #setSysSchema(Boolean)} for details.
     */
    public Boolean getSysSchema() {
        return getPropAsBoolean(sysSchema);
    }

    @Override
    public boolean isWrapperFor(Class<?> _iface) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Specifies the path to the Access database file.
     * <p>
     * For example: C:/folder/database.accdb
     *
     * @param _accessPath path
     */
    public void setAccessPath(String _accessPath) {
        accessPath = _accessPath;
    }

    public void setUser(String _user) {
        userPass.put(user, _user);
    }

    public void setPassword(String _password) {
        userPass.put(password, _password);
    }

    /**
     * Controls the order in which columns are returned by operations like SELECT *. The default is "DATA". See the
     * UCanAccess website for details.
     *
     * @param _value Valid values for this parameter are: "DATA", or "DISPLAY".
     * @since 2.0.9
     */
    public void setColumnOrder(String _value) {
        setProp(columnOrder, _value);
    }

    /**
     * Controls what is returned by concatenations involving null values. This setting affects <i>all</i> concatenation
     * operators (&amp;, +, ||).
     *
     * @param _value
     *            <ul>
     *            <li><b>false</b> (the default): null &amp; 'thing' returns 'thing'
     *            <li><b>true</b>: null &amp; 'thing' returns null
     *            </ul>
     * @since 3.0.0
     */
    public void setConcatNulls(Boolean _value) {
        setProp(concatNulls, _value);
    }

    /**
     * Encrypts the backing database.
     *
     * @param _value Setting this value to <b>true</b> tells UCanAccess to encrypt the HSQLDB backing database.
     * @since 1.0.4
     */
    public void setEncrypt(Boolean _value) {
        setProp(encrypt, _value);
    }

    /**
     * Enables case-insensitive string comparisons.
     *
     * @param _value (default = true)
     */
    public void setIgnoreCase(Boolean _value) {
        setProp(ignoreCase, _value);
    }

    /**
     * Releases the Access database and shuts down HSQLDB immediately after the Connection is closed.
     *
     * @param _value Setting this value to <b>true</b> tells UCanAccess to close the Access database file, shut down
     *            HSQLDB, and release all related resources (e.g., memory or disk space) as soon as the Connection is
     *            closed. Otherwise, UCanAccess will "hold on" to those resources for the
     *            {@link #setInactivityTimeout(Integer)} in case the Access database gets re-opened.
     */
    public void setImmediatelyReleaseResources(Boolean _value) {
        setProp(immediatelyReleaseResources, _value);
    }

    /**
     * For an in-memory backing database (memory=true), this parameter controls the time (in minutes, default=2), that
     * HSQLDB remains active and the in-memory database remains available after the last connection is closed.
     *
     * @param _value (in minutes)
     * @since 0.1.1
     */
    public void setInactivityTimeout(Integer _value) {
        setProp(inactivityTimeout, _value);
    }

    /**
     * Specifies the name of the custom class to be used by Jackcess when opening the Access database. Mainly for use
     * with Jackcess Encrypt to open encrypted databases. See the UCanAccess website for details.
     *
     * @param _value name of custom class
     * @since 0.0.2
     */
    public void setJackcessOpener(String _value) {
        setProp(jackcessOpener, _value);
    }

    /**
     * Specifies the path where a persistent copy of the HSQLDB backing database files should be stored. Implies
     * memory=false. See the UCanAccess website for details.
     *
     * @param _value path
     * @since 2.0.2
     */
    public void setKeepMirror(String _value) {
        setProp(keepMirror, _value);
    }

    /**
     * Controls the allocation unit size for "OLE Object" (i.e., BLOB) data in the HSQLDB backing database. See the
     * UCanAccess website for details.
     *
     * @param _value Values allowed are 1, 2, 4, 8, 16, or 32 (the unit is kB). Default is 2 if memory=true, and 32 (the
     *            HSQLDB default) otherwise.
     * @since 2.0.9.4
     */
    public void setLobScale(Integer _value) {
        setProp(lobScale, _value);
    }

    @Override
    public void setLoginTimeout(int _value) {
        loginTimeout = _value;
    }

    @Override
    public void setLogWriter(PrintWriter _logWriter) {
        logWriter = _logWriter;
    }

    /**
     * Controls whether the HSQLDB backing database is created in memory (the default) or on disk.
     *
     * @param _value Create the backing database in memory (value=true, the default) or on disk (value=false). Creating
     *            the backing database on disk will be slower, but it can greatly reduce JVM memory usage.
     */
    public void setMemory(Boolean _value) {
        setProp(memory, _value);
    }

    /**
     * Specifies the path where the temporary HSQLDB backing database files should be stored. Implies memory=false. To
     * create a persistent copy of the backing database, use {@link #setKeepMirror(String)} instead. See the UCanAccess
     * website for details.
     *
     * @param _value value
     * @since 2.0.9.3
     */
    public void setMirrorFolder(String _value) {
        setProp(mirrorFolder, _value);
    }

    /**
     * Creates a new, empty Access database in the specified format
     * if the database specified by {@link #setAccessPath(String)} does not exist.
     *
     * @param _version database versions, valid values are defined in enum {@link AccessVersion} ("V2000", "V2003", "V2007" etc.).
     */
    public void setNewDatabaseVersion(String _version) {
        AccessVersion accVersion = null;
        if (_version != null && !_version.isEmpty()) {
            accVersion = AccessVersion.parse(_version);
            if (accVersion == null) {
                UcanaccessRuntimeException.throwNow("Valid version required: " + _version);
            }
        }
        setNewDatabaseVersion(accVersion);
    }

    public void setNewDatabaseVersion(AccessVersion _version) {
        setProp(newDatabaseVersion, _version == null ? null : _version.name());
    }

    /**
     * Opens the Access database in "exclusive" mode.
     *
     * @param _value Setting this value to <b>true</b> tells UCanAccess to open the Access database as "exclusive"
     *            (preventing other processes from opening it at the same time).
     */
    public void setOpenExclusive(Boolean _value) {
        setProp(openExclusive, _value);
    }

    /**
     * Prevents unnecessary re-loading of backing database under very specific circumstances.<br>
     * USE WITH CAUTION! See the UCanAccess website for details.
     *
     * @param _value Setting this value to <b>true</b> prevents UCanAccess from unnecessarily re-loading the backing
     *            database under very specific circumstances.
     * @since 3.0.0
     */
    public void setPreventReloading(Boolean _value) {
        setProp(preventReloading, _value);
    }

    private void setProp(Metadata.Property _key, Object _value) {
        if (_key == null) {
            return;
        } else if (_value == null) {
            props.remove(_key);
            return;
        }
        if (!_key.isValidValue(_value)) {
            throw new UcanaccessRuntimeException("Invalid value '" + _value + "' for property " + _key);
        }
        props.put(_key, _value.toString());
    }

    /**
     * For this connection, temporarily re-directs linked tables in the Access database to point to a different Access
     * database. See the UCanAccess website for details.
     *
     * @param _value value
     * @since 2.0.2
     */
    public void setReMap(String _value) {
        setProp(reMap, _value);
    }

    /**
     * Exposes the HSQLDB catalog and schema names (e.g., "PUBLIC") in DatabaseMetadata.
     *
     * @param _value (default = false)
     */
    public void setShowSchema(Boolean _value) {
        setProp(showSchema, _value);
    }

    /**
     * Reduces memory consumption by not creating simple indexes in the backing database.
     *
     * @param _value Setting this value to <b>true</b> tells UCanAccess to skip the creation of simple indexes (not
     *            associated with a constraint). It doesn't have an effect on referential integrity constraints (i.e.,
     *            Index Unique, Foreign Key or Primary Key).
     * @since 2.0.9.4
     */
    public void setSkipIndexes(Boolean _value) {
        setProp(skipIndexes, _value);
    }

    /**
     * Exposes the Access system tables in a read-only schema named "SYS".
     *
     * @param _value (default = false)
     */
    public void setSysSchema(Boolean _value) {
        setProp(sysSchema, _value);
    }

    @Override
    public <T> T unwrap(Class<T> _iface) {
        throw new UnsupportedOperationException("Not supported yet");
    }

}
