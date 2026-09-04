package net.ucanaccess.jdbc;

import java.util.LinkedHashMap;
import java.util.Map;

public class NormalizedSQL {
    private String                    sql;
    private final Map<String, String> aliases = new LinkedHashMap<>();

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, String> getAliases() {
        return aliases;
    }

    public String put(String key, String value) {
        return aliases.put(key, value);
    }

    @Override
    public String toString() {
        return sql;
    }

}
