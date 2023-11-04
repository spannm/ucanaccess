package net.ucanaccess.jdbc;

import java.util.LinkedHashMap;
import java.util.Map;

public class NormalizedSQL {
    private String                    sql;
    private final Map<String, String> aliases = new LinkedHashMap<>();

    public String getSql() {
        return sql;
    }

    public void setSql(String _sql) {
        sql = _sql;
    }

    public Map<String, String> getAliases() {
        return aliases;
    }

    public String put(String _key, String _value) {
        return aliases.put(_key, _value);
    }

    @Override
    public String toString() {
        return sql;
    }

}
