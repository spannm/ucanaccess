package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;

public class CreateIndexCommand implements ICommand {
    private String indexName;
    private String tableName;
    private String execId;

    public CreateIndexCommand(String _indexName, String _tableName, String _execId) {
        this.indexName = _indexName;
        this.tableName = _tableName;
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
            p2a.createIndex(tableName, indexName);
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
        return null;
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        return null;
    }
}
