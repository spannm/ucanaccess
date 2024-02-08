package net.ucanaccess.exception;

import net.ucanaccess.jdbc.IUcanaccessErrorCodes;
import org.hsqldb.error.ErrorCode;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The sql exception specific to {@code Ucanaccess}.
 */
public class UcanaccessSQLException extends SQLException {

    static final String         MSG_PREFIX                   = "UCAExc:";

    private static final long   serialVersionUID             = -1432048647665807662L;
    private static final String UCANACCESS_GENERIC_ERROR_STR = String.valueOf(IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);

    public UcanaccessSQLException() {
    }

    public UcanaccessSQLException(String _reason, String _sqlState, int _vendorCode, Throwable _cause) {
        super(_reason, _sqlState, _vendorCode, _cause);
    }

    public UcanaccessSQLException(String _reason) {
        this(_reason, UCANACCESS_GENERIC_ERROR_STR, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(String _reason, Object... _args) {
        this(String.format(_reason, _args), UCANACCESS_GENERIC_ERROR_STR, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(String _reason, String _sqlState, int _vendorCode) {
        super(_reason, _sqlState, _vendorCode, null);
    }

    public UcanaccessSQLException(String _reason, String _sqlState, Throwable _cause) {
        super(_reason, _sqlState, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, _cause);
    }

    public UcanaccessSQLException(String _reason, Throwable _cause) {
        super(_reason, UCANACCESS_GENERIC_ERROR_STR,
            IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, _cause);
    }

    public UcanaccessSQLException(String _reason, String _sqlState) {
        super(_reason, _sqlState, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(Throwable _cause) {
        super(explainCause(_cause),
            _cause instanceof SQLException ? ((SQLException) _cause).getSQLState() : UCANACCESS_GENERIC_ERROR_STR,
            _cause instanceof SQLException ? ((SQLException) _cause).getErrorCode() : IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR,
            _cause);
    }

    public static String explainCause(Throwable _cause) {
        if (_cause instanceof SQLException) {
            SQLException se = (SQLException) _cause;
            if (se.getErrorCode() == -ErrorCode.X_42562) {
                return _cause.getMessage()
                    + " This exception may happen if you add integers representing units of time directly to datetime values "
                    + "using the arithmetic plus operator but without specifying the unit of date."
                    + System.lineSeparator()
                    + "In this specific case you have to use, for example, <dateColumn> + 1 DAY.";
            }
        }
        return _cause.getMessage();
    }

    String addVersionInfo(String _message) {
        if (_message != null && _message.startsWith(MSG_PREFIX)) {
            return _message;
        }
        return MSG_PREFIX
            + "::"
            + Optional.ofNullable(getClass().getPackage().getImplementationVersion()).orElse("5.1.0")
            + " "
            + (_message == null || _message.isBlank() ? "(n/a)" : _message.trim());
    }

    @Override
    public String getLocalizedMessage() {
        return addVersionInfo(super.getLocalizedMessage());
    }

    @Override
    public String getMessage() {
        return addVersionInfo(super.getMessage());
    }

    /**
     * Wraps the specified exception into a {@link UcanaccessSQLException}
     * unless its type can be cast to {@link UcanaccessSQLException}.
     *
     * @param <T> the type of exception to wrap
     * @param _t the exception to wrap
     * @return wrapped exception or parameter if can be cast to {@link UcanaccessSQLException}
     */
    public static final <T extends Throwable> UcanaccessSQLException wrap(T _t) {
        return UcanaccessSQLException.class.isInstance(_t)
            ? UcanaccessSQLException.class.cast(_t)
            : new UcanaccessSQLException(_t);
    }

    public static final <T extends UcanaccessSQLException> void throwIf(Supplier<Boolean> _condition, Supplier<T> _exceptionSupplier) throws T {
        if (_condition.get()) {
            throw _exceptionSupplier.get();
        }
    }

}
