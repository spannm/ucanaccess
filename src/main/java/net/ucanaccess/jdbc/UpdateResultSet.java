package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class UpdateResultSet extends AbstractExecuteResultSet {

    protected UpdateResultSet(UcanaccessResultSet _resultSet) {
        super(_resultSet, ResultSet::updateRow);
    }

}
