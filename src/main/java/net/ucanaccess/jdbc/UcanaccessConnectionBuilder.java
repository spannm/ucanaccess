package net.ucanaccess.jdbc;

import net.ucanaccess.util.Try;

import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A build for Ucanaccess database urls and connections.
 */
public final class UcanaccessConnectionBuilder {

    private String              user              = "ucanaccess";
    private String              password;
    private String              dbPath;
    private boolean             ignoreCase;
    private long                inactivityTimeout = -1;
    private String              columnOrder;
    private Map<String, Object> addParms          = new LinkedHashMap<>();

    public UcanaccessConnectionBuilder withUser(String _user) {
        user = _user;
        return this;
    }

    public UcanaccessConnectionBuilder withPassword(String _password) {
        password = _password;
        return this;
    }

    public UcanaccessConnectionBuilder withoutUserPass() {
        user = "";
        password = "";
        return this;
    }

    public UcanaccessConnectionBuilder withDbPath(String _dbPath) {
        dbPath = _dbPath;
        return this;
    }

    public UcanaccessConnectionBuilder withIgnoreCase(boolean _ignoreCase) {
        ignoreCase = _ignoreCase;
        return this;
    }

    public UcanaccessConnectionBuilder withInactivityTimeout(long _inactivityTimeout) {
        inactivityTimeout = _inactivityTimeout;
        return this;
    }

    public UcanaccessConnectionBuilder withColumnOrder(String _columnOrder) {
        columnOrder = _columnOrder;
        return this;
    }

    public UcanaccessConnectionBuilder withParm(String _key, Object _value) {
        Objects.requireNonNull(_key, "Key required");
        Objects.requireNonNull(_value, "Value required");
        addParms.put(_key, _value);
        return this;
    }

    public String buildUrl() {
        Objects.requireNonNull(dbPath, "Database path required");

        String url = UcanaccessDriver.URL_PREFIX + dbPath;

        if (ignoreCase) {
            url += ";ignoreCase=" + ignoreCase;
        }
        if (inactivityTimeout > -1) {
            url += ";inactivityTimeout=" + inactivityTimeout;
        } else {
            url += ";immediatelyReleaseResources=true";
        }
        if (columnOrder != null) {
            url += ";columnOrder=" + columnOrder;
        }
        if (!addParms.isEmpty()) {
            url += ";" + addParms.entrySet().stream().map(e -> e.getKey() + '=' + e.getValue()).collect(Collectors.joining(";"));
        }
        return url;
    }

    public UcanaccessConnection build() {
        Try.catching(() -> Class.forName(UcanaccessDriver.class.getName()))
            .orThrow(UcanaccessRuntimeException::new);

        return Try.catching(() -> DriverManager.getConnection(buildUrl(), user, password))
            .map(UcanaccessConnection.class::cast).orThrow();
    }

}
