package net.ucanaccess.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

public class UcanaccessSavepoint implements Savepoint {
    private Savepoint wrapped;

    public UcanaccessSavepoint(Savepoint _wrapped) {
        wrapped = _wrapped;
    }

    @Override
    public int getSavepointId() throws SQLException {
        try {
            return wrapped.getSavepointId();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public String getSavepointName() throws SQLException {
        try {
            return wrapped.getSavepointName();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    Savepoint getWrapped() {
        return wrapped;
    }

}
