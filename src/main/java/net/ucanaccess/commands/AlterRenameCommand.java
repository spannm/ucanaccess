package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class AlterRenameCommand implements ICommand {
    private final String execId;
    private final String oldTableName;
    private final String newTableName;

    public AlterRenameCommand(String _oldTableName, String _newTableName, String _execId) {
        oldTableName = _oldTableName;
        newTableName = _newTableName;
        execId = _execId;
    }

    @Override
    public String getExecId() {
        return execId;
    }

    @Override
    public String getTableName() {
        return oldTableName;
    }

    @Override
    public CommandType getType() {
        return CommandType.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> new Persist2Jet().renameTable(oldTableName, newTableName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }

}
