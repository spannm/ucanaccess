package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class CreatePrimaryKeyCommand implements ICommand {
    private String tableName;
    private String execId;

    public CreatePrimaryKeyCommand(String _tableName, String _execId) {
        tableName = _tableName;
        execId = _execId;
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
        Try.catching(() -> new Persist2Jet().createPrimaryKey(tableName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
