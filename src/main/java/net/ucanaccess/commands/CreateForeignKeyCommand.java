package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

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
    public CommandType getType() {
        return CommandType.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> new Persist2Jet().createForeignKey(tableName, referencedTable, relationshipName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
