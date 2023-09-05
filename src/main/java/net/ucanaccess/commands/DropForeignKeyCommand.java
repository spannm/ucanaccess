package net.ucanaccess.commands;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;

public class DropForeignKeyCommand implements ICommand {

    private String execId;
    private String relationshipName;

    public DropForeignKeyCommand(String _execId, String _relationshipName) {
        this.execId = _execId;
        this.relationshipName = _relationshipName;
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
    public TYPES getType() {
        return TYPES.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            Persist2Jet p2a = new Persist2Jet();
            p2a.dropForeignKey(this.relationshipName);
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
