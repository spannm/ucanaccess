package net.ucanaccess.commands;

import java.io.IOException;
import java.sql.SQLException;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

public class AlterRenameCommand implements ICommand {
    private String execId;
    private String oldTableName;
    private String newTableName;

    public AlterRenameCommand(String _oldTableName, String _newTableName, String _execId) {
        this.oldTableName = _oldTableName;
        this.newTableName = _newTableName;
        this.execId = _execId;
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
    public TYPES getType() {
        return TYPES.DDL;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            Persist2Jet p2a = new Persist2Jet();
            p2a.renameTable(oldTableName, newTableName);
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
