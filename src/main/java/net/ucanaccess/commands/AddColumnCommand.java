package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

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

    public AddColumnCommand(String tableName, String columnName, String execId, Map<String, String> columnMap,
            String[] types, String[] defaults, Boolean[] notNulls) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.types = types;
        this.defaults = defaults;
        this.notNulls = notNulls;
        this.columnMap = columnMap;
        this.execId = execId;
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
    public CommandType getType() {
        return CommandType.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> new Persist2Jet().addColumn(tableName, columnName, columnMap, types, defaults, notNulls))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }

}
