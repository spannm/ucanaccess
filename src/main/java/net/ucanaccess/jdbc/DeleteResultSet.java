package net.ucanaccess.jdbc;

import java.sql.SQLException;

public class DeleteResultSet extends AbstractExecute {
    protected DeleteResultSet(UcanaccessResultSet resultSet) {
        super(resultSet);
    }

    public void execute() throws SQLException {
        executeBase();
    }

    @Override
    public Object executeWrapped() throws SQLException {
        super.getWrappedResultSet().deleteRow();
        return true;

    }
}
