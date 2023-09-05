package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class CreateTableCommand implements ICommand {
    private String              execId;
    private String              tableName;
    private String[]            types;
    private String[]            defaults;
    private Boolean[]           notNulls;
    private Map<String, String> columnMap;

    public CreateTableCommand(String _tableName, String _execId) {
        this.tableName = _tableName;
        this.execId = _execId;
    }

    public CreateTableCommand(String tn, String execId2, Map<String, String> _columnMap, String[] _types,
            String[] _defaults, Boolean[] _notNulls) {
        this(tn, execId2);
        this.types = _types;
        this.defaults = _defaults;
        this.notNulls = _notNulls;
        this.columnMap = _columnMap;
    }

    public CreateTableCommand(String tn, String execId2, Map<String, String> columnMap2) {
        this(tn, execId2, columnMap2, null, null, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CreateTableCommand other = (CreateTableCommand) obj;
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        } else if (!tableName.equals(other.tableName)) {
            return false;
        }
        return true;
    }

    @Override
    public String getExecId() {
        return execId;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public TYPES getType() {
        return TYPES.DDL;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            Persist2Jet p2a = new Persist2Jet();
            p2a.createTable(tableName, columnMap, types, defaults, notNulls);
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
