package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.sql.SQLException;

public class DropForeignKeyCommand implements ICommand {

    private final String execId;
    private final String relationshipName;

    public DropForeignKeyCommand(String execId, String relationshipName) {
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
        throw new UnsupportedOperationException("The getTableName method is not applicable to this object.");
    }

    @Override
    public CommandType getType() {
        return CommandType.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> new Persist2Jet().dropForeignKey(relationshipName))
            .orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public IFeedbackAction rollback() {
        return null;
    }
}
