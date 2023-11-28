package net.ucanaccess.jdbc;

import net.ucanaccess.util.IThrowingConsumer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public abstract class AbstractExecuteResultSet extends AbstractExecute {

    private final IThrowingConsumer<ResultSet, SQLException> executeConsumer;

    protected AbstractExecuteResultSet(UcanaccessResultSet _resultSet, IThrowingConsumer<ResultSet, SQLException> _executeConsumer) {
        super(_resultSet);
        executeConsumer = Objects.requireNonNull(_executeConsumer, "Consumer required");
    }

    public final void execute() throws SQLException {
        executeBase();
    }

    @Override
    public final Object executeWrapped() throws SQLException {
        executeConsumer.accept(getWrappedResultSet());
        return true;
    }

}
