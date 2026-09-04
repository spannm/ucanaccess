package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class DeleteResultSet extends AbstractExecuteResultSet {

    protected DeleteResultSet(UcanaccessResultSet resultSet) {
        super(resultSet, ResultSet::deleteRow);
    }

}
