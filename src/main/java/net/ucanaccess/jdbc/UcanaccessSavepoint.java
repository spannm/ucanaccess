package net.ucanaccess.jdbc;

import net.ucanaccess.util.Try;

import java.sql.SQLException;
import java.sql.Savepoint;

public class UcanaccessSavepoint implements Savepoint {
    private Savepoint wrapped;

    public UcanaccessSavepoint(Savepoint _wrapped) {
        wrapped = _wrapped;
    }

    @Override
    public int getSavepointId() throws SQLException {
        return Try.catching(wrapped::getSavepointId).orThrow();
    }

    @Override
    public String getSavepointName() throws SQLException {
        return Try.catching(wrapped::getSavepointName).orThrow();
    }

    Savepoint getWrapped() {
        return wrapped;
    }

}
