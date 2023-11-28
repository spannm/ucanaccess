package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class InsertResultSet extends AbstractExecuteResultSet {

    protected InsertResultSet(UcanaccessResultSet _resultSet) {
        super(_resultSet, ResultSet::insertRow);
    }

}
