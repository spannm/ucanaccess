package net.ucanaccess.jdbc;

import java.util.HashMap;
import java.util.Map;

public class NormalizedSQL {
    private String                    sql;
    private final Map<String, String> aliases = new HashMap<>();

    public String getSql() {
        return sql;
    }

    public void setSql(String _sql) {
        sql = _sql;
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
