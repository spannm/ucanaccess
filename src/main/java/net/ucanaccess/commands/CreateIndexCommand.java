package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class CreateIndexCommand implements ICommand {
    private String indexName;
    private String tableName;
    private String execId;

    public CreateIndexCommand(String _indexName, String _tableName, String _execId) {
        indexName = _indexName;
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
        Try.catching(() -> new Persist2Jet().createIndex(tableName, indexName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
