package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.type.ColumnOrder;
import net.ucanaccess.util.Try;

import java.io.File;
import java.sql.DriverManager;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A builder for Ucanaccess database urls and connections.
 *
 * @author Markus Spann
 * @since v5.1.0
 */
public final class UcanaccessConnectionBuilder {

    private String                      dbPath;
    private final Map<Property, Object> props = new EnumMap<>(Property.class);

    public UcanaccessConnectionBuilder withDbPath(String path) {
        dbPath = path;
        return this;
    }

    public UcanaccessConnectionBuilder withDbPath(File path) {
        dbPath = Optional.ofNullable(path).map(File::getAbsolutePath).orElse(null);
        return this;
    }

    public UcanaccessConnectionBuilder withUser(String user) {
        return withProp(Property.user, user);
    }

    public UcanaccessConnectionBuilder withPassword(String pass) {
        return withProp(password, pass);
    }

    public UcanaccessConnectionBuilder withoutUserPass() {
        props.remove(user);
        props.remove(password);
        return this;
    }

    public UcanaccessConnectionBuilder withColumnOrderData() {
        return withProp(columnOrder, ColumnOrder.DATA);
    }

    public UcanaccessConnectionBuilder withColumnOrderDisplay() {
        return withProp(columnOrder, ColumnOrder.DISPLAY);
    }

    public UcanaccessConnectionBuilder withConcatNulls(boolean concatNulls) {
        return withProp(Property.concatNulls, concatNulls);
    }

    public UcanaccessConnectionBuilder withIgnoreCase(boolean ignoreCase) {
        return withProp(Property.ignoreCase, ignoreCase);
    }

    public UcanaccessConnectionBuilder withImmediatelyReleaseResources() {
        return withProp(immediatelyReleaseResources, true);
    }

    public UcanaccessConnectionBuilder withInactivityTimeout(int inactivityTimeout) {
        return withProp(Property.inactivityTimeout, inactivityTimeout);
    }

    public UcanaccessConnectionBuilder withMemory() {
        return withProp(memory, true);
    }

    public UcanaccessConnectionBuilder withNewDatabaseVersion(AccessVersion version) {
        return withProp(newDatabaseVersion, version == null ? null : version.name());
    }

    public UcanaccessConnectionBuilder withNewDatabaseVersion(String version) {
        AccessVersion accVersion = null;
        if (version != null) {
            accVersion = AccessVersion.parse(version);
            if (accVersion == null) {
                UcanaccessRuntimeException.throwNow("Valid version required: " + version);
            }
        }
        return withProp(newDatabaseVersion, accVersion);
    }

    public UcanaccessConnectionBuilder withProp(Metadata.Property prop, Object value) {
        UcanaccessRuntimeException.requireNonNull(prop, "Property required");

        String val = Optional.ofNullable(value).map(Object::toString).orElse("");
        props.put(prop, val);

        return this;
    }

    public String getUrl() {
        UcanaccessRuntimeException.requireNonNull(dbPath, "Database path required");

        String url = UcanaccessDriver.URL_PREFIX + dbPath;

        String propsStr = propsToString(";");
        if (!propsStr.isEmpty()) {
            url += ";" + propsStr;
        }
        return url;
    }

    public String getUser() {
        return (String) props.get(user);
    }

    public String getPassword() {
        return (String) props.get(password);
    }

    public UcanaccessConnection build() {
        Try.catching(() -> Class.forName(UcanaccessDriver.class.getName()))
            .orThrow(UcanaccessRuntimeException::new);

        return Try.catching(() -> DriverManager.getConnection(getUrl(), getUser(), getPassword()))
            .map(UcanaccessConnection.class::cast).orThrow();
    }

    /**
     * Creates a semicolon-delimited string of all properties and their values, without properties {@code user} and {@code password},
     * and performing special handling on certain properties.
     *
     * @param delimiter delimiter
     * @return property string
     */
    String propsToString(CharSequence delimiter) {
        Map<Property, Object> copy = new LinkedHashMap<>(props);

        copy.remove(user);
        copy.remove(password);

        // special handling of certain properties
        if (Integer.parseInt(copy.getOrDefault(inactivityTimeout, -1).toString()) > -1) {
            copy.remove(immediatelyReleaseResources);
        } else {
            copy.put(immediatelyReleaseResources, true);
        }

        return copy.entrySet().stream()
            .map(e -> e.getKey().name() + '=' + e.getValue())
            .collect(Collectors.joining(delimiter));
    }

    @Override
    public String toString() {
        return String.format("%s[user=%s, dbPath=%s, props={%s}]",
            getClass().getSimpleName(), props.get(user), dbPath, propsToString(", "));
    }

}
