package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class InsertResultSet extends AbstractExecuteResultSet {

    protected InsertResultSet(UcanaccessResultSet resultSet) {
        super(resultSet, ResultSet::insertRow);
    }

}
