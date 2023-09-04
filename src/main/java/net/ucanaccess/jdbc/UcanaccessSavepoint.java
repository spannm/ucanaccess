package net.ucanaccess.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

public class UcanaccessSavepoint implements Savepoint {
    private Savepoint wrapped;

    public UcanaccessSavepoint(Savepoint _wrapped) {
        this.wrapped = _wrapped;
    }

    @Override
    public int getSavepointId() throws SQLException {
        try {
            return wrapped.getSavepointId();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public String getSavepointName() throws SQLException {
        try {
            return wrapped.getSavepointName();
        } catch (SQLException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    Savepoint getWrapped() {
        return wrapped;
    }

}
