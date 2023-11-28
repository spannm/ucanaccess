package net.ucanaccess.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Execute extends AbstractExecute {

    public Execute(UcanaccessPreparedStatement _statement) {
        super(_statement, CommandType.PREPARED_STATEMENT, null, 0, null, null);
    }

    public Execute(UcanaccessStatement _statement, String _sql) {
        super(_statement, CommandType.NO_ARGUMENTS, _sql, 0, null, null);
    }

    public Execute(UcanaccessStatement _statement, String _sql, int _autoGeneratedKeys) {
        super(_statement, CommandType.WITH_AUTO_GENERATED_KEYS, _sql, _autoGeneratedKeys, null, null);
    }

    public Execute(UcanaccessStatement _statement, String _sql, int[] _indexes) {
        super(_statement, CommandType.WITH_INDEXES, _sql, 0, null, _indexes);
    }

    public Execute(UcanaccessStatement _statement, String _sql, String[] _columnNames) {
        super(_statement, CommandType.WITH_COLUMNS_NAME, _sql, 0, _columnNames, null);
    }

    public boolean execute() throws SQLException {
        return (Boolean) executeBase();
    }

    @Override
    public Object executeWrapped() throws SQLException {
        Statement w = getWrappedStatement();
        switch (getCommandType()) {
        case PREPARED_STATEMENT:
            return ((PreparedStatement) w).execute();
        case NO_ARGUMENTS:
            return w.execute(getSql());
        case WITH_COLUMNS_NAME:
            return w.execute(getSql(), getColumnNames());
        case WITH_AUTO_GENERATED_KEYS:
            return w.execute(getSql(), getAutoGeneratedKeys());
        case WITH_INDEXES:
            return w.execute(getSql(), getIndexes());
        default:
            return false;
        }
    }

}
