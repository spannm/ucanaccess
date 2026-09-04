package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class CreateForeignKeyCommand implements ICommand {

    private final String tableName;
    private final String referencedTable;
    private final String execId;
    private final String relationshipName;

    public CreateForeignKeyCommand(String tableName, String referencedTable, String execId, String relationshipName) {
        this.tableName = tableName;
        this.referencedTable = referencedTable;
        this.execId = execId;
        this.relationshipName = relationshipName;
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
