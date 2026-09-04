package net.ucanaccess.jdbc;

import java.sql.ResultSet;

public class UpdateResultSet extends AbstractExecuteResultSet {

    protected UpdateResultSet(UcanaccessResultSet resultSet) {
        super(resultSet, ResultSet::updateRow);
    }

}
