package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;

public class DropTableCommand implements ICommand {
    private final String execId;
    private final String tableName;

    public DropTableCommand(String tableName, String execId) {
        this.tableName = tableName;
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
        try {
            Persist2Jet p2a = new Persist2Jet();
            p2a.dropTable(tableName);
        } catch (IOException ex) {
            throw new UcanaccessSQLException(ex);
        }
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
