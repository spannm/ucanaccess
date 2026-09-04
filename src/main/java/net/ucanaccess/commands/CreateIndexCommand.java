package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class CreateIndexCommand implements ICommand {
    private final String indexName;
    private final String tableName;
    private final String execId;

    public CreateIndexCommand(String indexName, String tableName, String execId) {
        this.indexName = indexName;
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
        Try.catching(() -> new Persist2Jet().createIndex(tableName, indexName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
