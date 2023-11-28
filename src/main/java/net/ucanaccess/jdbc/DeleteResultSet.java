package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class DeleteResultSet extends AbstractExecuteResultSet {

    protected DeleteResultSet(UcanaccessResultSet _resultSet) {
        super(_resultSet, ResultSet::deleteRow);
    }

}
