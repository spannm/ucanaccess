package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.type.ColumnOrder;
import net.ucanaccess.util.Try;

import java.sql.DriverManager;
import java.util.*;
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

    public UcanaccessConnectionBuilder withDbPath(String _dbPath) {
        dbPath = _dbPath;
        return this;
    }

    public UcanaccessConnectionBuilder withUser(String _user) {
        return withProp(user, _user);
    }

    public UcanaccessConnectionBuilder withPassword(String _pass) {
        return withProp(password, _pass);
    }

    public UcanaccessConnectionBuilder withoutUserPass() {
        props.remove(ignoreCase);
        props.remove(password);
        return this;
    }

    public UcanaccessConnectionBuilder withColumnOrderData() {
        return withProp(columnOrder, ColumnOrder.DATA);
    }

    public UcanaccessConnectionBuilder withColumnOrderDisplay() {
        return withProp(columnOrder, ColumnOrder.DISPLAY);
    }

    public UcanaccessConnectionBuilder withConcatNulls(boolean _concatNulls) {
        return withProp(concatNulls, _concatNulls);
    }

    public UcanaccessConnectionBuilder withIgnoreCase(boolean _ignoreCase) {
        return withProp(ignoreCase, _ignoreCase);
    }

    public UcanaccessConnectionBuilder withImmediatelyReleaseResources() {
        return withProp(immediatelyReleaseResources, true);
    }

    public UcanaccessConnectionBuilder withInactivityTimeout(int _inactivityTimeout) {
        return withProp(inactivityTimeout, _inactivityTimeout);
    }

    public UcanaccessConnectionBuilder withMemory() {
        return withProp(memory, true);
    }

    public UcanaccessConnectionBuilder withNewDatabaseVersionProp(String _name) {
        return withProp(newDatabaseVersion, _name);
    }

    public UcanaccessConnectionBuilder withProp(Metadata.Property _prop, Object _value) {
        Objects.requireNonNull(_prop, "Property required");

        String val = Optional.ofNullable(_value).map(Object::toString).orElse("");
        props.put(_prop, val);

        return this;
    }

    public String getUrl() {
        Objects.requireNonNull(dbPath, "Database path required");

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
     * @param _delimiter delimiter
     * @return property string
     */
    String propsToString(CharSequence _delimiter) {
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
            .collect(Collectors.joining(_delimiter));
    }

    @Override
    public String toString() {
        return String.format("%s[user=%s, dbPath=%s, props={%s}]",
            getClass().getSimpleName(), props.get(user), dbPath, propsToString(", "));
    }

}
