package net.ucanaccess.jdbc;

import java.sql.SQLException;

public class UpdateResultSet extends AbstractExecute {
    protected UpdateResultSet(UcanaccessResultSet resultSet) {
        super(resultSet);
    }

    public void execute() throws SQLException {
        executeBase();
    }

    @Override
    public Object executeWrapped() throws SQLException {
        super.getWrappedResultSet().updateRow();
        return true;
    }
}
