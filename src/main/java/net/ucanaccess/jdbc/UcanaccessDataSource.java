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

@SuppressWarnings("PMD.UnnecessaryFullyQualifiedName")
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
     * @param user user name
     * @param pass password
     * @return database connection
     */
    @Override
    public Connection getConnection(String user, String pass) throws SQLException {
        // do not store user/pass in member props
        Properties copy = props.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().name(), Entry::getValue, (v1, v2) -> v1, Properties::new));
        Optional.ofNullable(user).ifPresent(u -> copy.put(Property.user.name(), u));
        Optional.ofNullable(pass).ifPresent(p -> copy.put(Property.password.name(), p));
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
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }

    /**
     * See {@link #setPreventReloading(Boolean)} for details.
     */
    public Boolean getPreventReloading() {
        return getPropAsBoolean(preventReloading);
    }

    private Boolean getPropAsBoolean(Metadata.Property key) {
        return Optional.ofNullable(getPropAsString(key))
            .map(Boolean::valueOf)
            .orElse(null);
    }

    private Integer getPropAsInteger(Metadata.Property key) {
        return Optional.ofNullable(getPropAsString(key))
            .map(Integer::valueOf)
            .orElse(null);
    }

    private String getPropAsString(Metadata.Property key) {
        return key == null ? null : props.get(key);
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
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Specifies the path to the Access database file.
     * <p>
     * For example: C:/folder/database.accdb
     *
     * @param accessPath path
     */
    public void setAccessPath(String accessPath) {
        this.accessPath = accessPath;
    }

    public void setUser(String user) {
        userPass.put(Property.user, user);
    }

    public void setPassword(String password) {
        userPass.put(Property.password, password);
    }

    /**
     * Controls the order in which columns are returned by operations like SELECT *. The default is "DATA". See the
     * UCanAccess website for details.
     *
     * @param value Valid values for this parameter are: "DATA", or "DISPLAY".
     * @since 2.0.9
     */
    public void setColumnOrder(String value) {
        setProp(columnOrder, value);
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
        setProp(concatNulls, value);
    }

    /**
     * Encrypts the backing database.
     *
     * @param value Setting this value to <b>true</b> tells UCanAccess to encrypt the HSQLDB backing database.
     * @since 1.0.4
     */
    public void setEncrypt(Boolean value) {
        setProp(encrypt, value);
    }

    /**
     * Enables case-insensitive string comparisons.
     *
     * @param value (default = true)
     */
    public void setIgnoreCase(Boolean value) {
        setProp(ignoreCase, value);
    }

    /**
     * Releases the Access database and shuts down HSQLDB immediately after the Connection is closed.
     *
     * @param value Setting this value to <b>true</b> tells UCanAccess to close the Access database file, shut down
     *            HSQLDB, and release all related resources (e.g., memory or disk space) as soon as the Connection is
     *            closed. Otherwise, UCanAccess will "hold on" to those resources for the
     *            {@link #setInactivityTimeout(Integer)} in case the Access database gets re-opened.
     */
    public void setImmediatelyReleaseResources(Boolean value) {
        setProp(immediatelyReleaseResources, value);
    }

    /**
     * For an in-memory backing database (memory=true), this parameter controls the time (in minutes, default=2), that
     * HSQLDB remains active and the in-memory database remains available after the last connection is closed.
     *
     * @param value (in minutes)
     * @since 0.1.1
     */
    public void setInactivityTimeout(Integer value) {
        setProp(inactivityTimeout, value);
    }

    /**
     * Specifies the name of the custom class to be used by Jackcess when opening the Access database. Mainly for use
     * with Jackcess Encrypt to open encrypted databases. See the UCanAccess website for details.
     *
     * @param value name of custom class
     * @since 0.0.2
     */
    public void setJackcessOpener(String value) {
        setProp(jackcessOpener, value);
    }

    /**
     * Specifies the path where a persistent copy of the HSQLDB backing database files should be stored. Implies
     * memory=false. See the UCanAccess website for details.
     *
     * @param value path
     * @since 2.0.2
     */
    public void setKeepMirror(String value) {
        setProp(keepMirror, value);
    }

    /**
     * Controls the allocation unit size for "OLE Object" (i.e., BLOB) data in the HSQLDB backing database. See the
     * UCanAccess website for details.
     *
     * @param value Values allowed are 1, 2, 4, 8, 16, or 32 (the unit is kB). Default is 2 if memory=true, and 32 (the
     *            HSQLDB default) otherwise.
     * @since 2.0.9.4
     */
    public void setLobScale(Integer value) {
        setProp(lobScale, value);
    }

    @Override
    public void setLoginTimeout(int value) {
        loginTimeout = value;
    }

    @Override
    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    /**
     * Controls whether the HSQLDB backing database is created in memory (the default) or on disk.
     *
     * @param value Create the backing database in memory (value=true, the default) or on disk (value=false). Creating
     *            the backing database on disk will be slower, but it can greatly reduce JVM memory usage.
     */
    public void setMemory(Boolean value) {
        setProp(memory, value);
    }

    /**
     * Specifies the path where the temporary HSQLDB backing database files should be stored. Implies memory=false. To
     * create a persistent copy of the backing database, use {@link #setKeepMirror(String)} instead. See the UCanAccess
     * website for details.
     *
     * @param value value
     * @since 2.0.9.3
     */
    public void setMirrorFolder(String value) {
        setProp(mirrorFolder, value);
    }

    /**
     * Creates a new, empty Access database in the specified format
     * if the database specified by {@link #setAccessPath(String)} does not exist.
     *
     * @param version database versions, valid values are defined in enum {@link AccessVersion} ("V2000", "V2003", "V2007" etc.).
     */
    public void setNewDatabaseVersion(String version) {
        AccessVersion accVersion = null;
        if (version != null && !version.isEmpty()) {
            accVersion = AccessVersion.parse(version);
            if (accVersion == null) {
                UcanaccessRuntimeException.throwNow("Valid version required: " + version);
            }
        }
        setNewDatabaseVersion(accVersion);
    }

    public void setNewDatabaseVersion(AccessVersion version) {
        setProp(newDatabaseVersion, version == null ? null : version.name());
    }

    /**
     * Opens the Access database in "exclusive" mode.
     *
     * @param value Setting this value to <b>true</b> tells UCanAccess to open the Access database as "exclusive"
     *            (preventing other processes from opening it at the same time).
     */
    public void setOpenExclusive(Boolean value) {
        setProp(openExclusive, value);
    }

    /**
     * Prevents unnecessary re-loading of backing database under very specific circumstances.<br>
     * USE WITH CAUTION! See the UCanAccess website for details.
     *
     * @param value Setting this value to <b>true</b> prevents UCanAccess from unnecessarily re-loading the backing
     *            database under very specific circumstances.
     * @since 3.0.0
     */
    public void setPreventReloading(Boolean value) {
        setProp(preventReloading, value);
    }

    private void setProp(Metadata.Property key, Object value) {
        if (key == null) {
            return;
        } else if (value == null) {
            props.remove(key);
            return;
        }
        if (!key.isValidValue(value)) {
            throw new UcanaccessRuntimeException("Invalid value '" + value + "' for property " + key);
        }
        props.put(key, value.toString());
    }

    /**
     * For this connection, temporarily re-directs linked tables in the Access database to point to a different Access
     * database. See the UCanAccess website for details.
     *
     * @param value value
     * @since 2.0.2
     */
    public void setReMap(String value) {
        setProp(reMap, value);
    }

    /**
     * Exposes the HSQLDB catalog and schema names (e.g., "PUBLIC") in DatabaseMetadata.
     *
     * @param value (default = false)
     */
    public void setShowSchema(Boolean value) {
        setProp(showSchema, value);
    }

    /**
     * Reduces memory consumption by not creating simple indexes in the backing database.
     *
     * @param value Setting this value to <b>true</b> tells UCanAccess to skip the creation of simple indexes (not
     *            associated with a constraint). It doesn't have an effect on referential integrity constraints (i.e.,
     *            Index Unique, Foreign Key or Primary Key).
     * @since 2.0.9.4
     */
    public void setSkipIndexes(Boolean value) {
        setProp(skipIndexes, value);
    }

    /**
     * Exposes the Access system tables in a read-only schema named "SYS".
     *
     * @param value (default = false)
     */
    public void setSysSchema(Boolean value) {
        setProp(sysSchema, value);
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException("Not supported yet");
    }

}
