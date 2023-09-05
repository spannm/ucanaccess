package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class AddColumnCommand implements ICommand {
    private final String              execId;
    private final String              tableName;
    private final String[]            types;
    private final String[]            defaults;
    private final Boolean[]           notNulls;
    private final Map<String, String> columnMap;
    private final String              columnName;

    public AddColumnCommand(String _tableName, String _columnName, String _execId, Map<String, String> _columnMap,
            String[] _types, String[] _defaults, Boolean[] _notNulls) {
        this.tableName = _tableName;
        this.columnName = _columnName;
        this.types = _types;
        this.defaults = _defaults;
        this.notNulls = _notNulls;
        this.columnMap = _columnMap;
        this.execId = _execId;
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
    public IFeedbackAction persist() throws SQLException {
        try {
            Persist2Jet p2a = new Persist2Jet();
            p2a.addColumn(tableName, columnName, columnMap, types, defaults, notNulls);
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
