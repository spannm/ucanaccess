package net.ucanaccess.jdbc;

import java.sql.SQLException;

public class InsertResultSet extends AbstractExecute {
    protected InsertResultSet(UcanaccessResultSet resultSet) {
        super(resultSet);
    }

    public void execute() throws SQLException {
        executeBase();
    }

    @Override
    public Object executeWrapped() throws SQLException {
        super.getWrappedResultSet().insertRow();
        return true;
    }
}
