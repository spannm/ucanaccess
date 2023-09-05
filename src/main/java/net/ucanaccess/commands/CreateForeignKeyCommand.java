package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;

public class CreateForeignKeyCommand implements ICommand {

    private String tableName;
    private String referencedTable;
    private String execId;
    private String relationshipName;

    public CreateForeignKeyCommand(String _tableName, String _referencedTable, String _execId, String _relationshipName) {
        tableName = _tableName;
        referencedTable = _referencedTable;
        execId = _execId;
        relationshipName = _relationshipName;
    }

    public String getRelationshipName() {
        return relationshipName;
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
            p2a.createForeignKey(tableName, referencedTable, relationshipName);
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
