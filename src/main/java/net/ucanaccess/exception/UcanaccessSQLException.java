package net.ucanaccess.exception;

import net.ucanaccess.jdbc.IUcanaccessErrorCodes;
import net.ucanaccess.util.VersionInfo;
import org.hsqldb.error.ErrorCode;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.BooleanSupplier;
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

    public UcanaccessSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }

    public UcanaccessSQLException(String reason) {
        this(reason, UCANACCESS_GENERIC_ERROR_STR, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(String reason, Object... args) {
        this(String.format(reason, args), UCANACCESS_GENERIC_ERROR_STR, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(String reason, String sqlState, int vendorCode) {
        super(reason, sqlState, vendorCode, null);
    }

    public UcanaccessSQLException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, cause);
    }

    public UcanaccessSQLException(String reason, Throwable cause) {
        super(reason, UCANACCESS_GENERIC_ERROR_STR,
            IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, cause);
    }

    public UcanaccessSQLException(String reason, String sqlState) {
        super(reason, sqlState, IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR, null);
    }

    public UcanaccessSQLException(Throwable cause) {
        super(explainCause(cause),
            cause instanceof SQLException ? ((SQLException) cause).getSQLState() : UCANACCESS_GENERIC_ERROR_STR,
            cause instanceof SQLException ? ((SQLException) cause).getErrorCode() : IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR,
            cause);
    }

    public static String explainCause(Throwable cause) {
        if (cause instanceof SQLException) {
            SQLException se = (SQLException) cause;
            if (se.getErrorCode() == -ErrorCode.X_42562) {
                return cause.getMessage()
                    + " This exception may happen if you add integers representing units of time directly to datetime values "
                    + "using the arithmetic plus operator but without specifying the unit of date."
                    + System.lineSeparator()
                    + "In this specific case you have to use, for example, <dateColumn> + 1 DAY.";
            }
        }
        return cause.getMessage();
    }

    String addVersionInfo(String message) {
        if (message != null && message.startsWith(MSG_PREFIX)) {
            return message;
        }

        String ver = VersionInfo.find(getClass()).getVersion();
        return (MSG_PREFIX
            + "::"
            + Optional.ofNullable(ver).orElse("x.y.z")
            + " "
            + (message == null || message.isBlank() ? "(n/a)" : message)).trim();
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
     * Wraps a {@link Throwable} into a {@link UcanaccessSQLException}.
     * <p>
     * If the throwable is already a {@link UcanaccessSQLException}, it's returned as is.
     * This is a convenience method calling {@link #wrap(String, Throwable)} without a reason prefix.
     * </p>
     *
     * @param <T> the type of the {@link Throwable} to wrap.
     * @param t the {@link Throwable} to wrap.
     * @return a {@link UcanaccessSQLException} instance.
     */
    public static final <T extends Throwable> UcanaccessSQLException wrap(T t) {
        return wrap(null, t);
    }

    /**
     * Wraps a {@link Throwable} into a {@link UcanaccessSQLException},
     * prepending an optional reason message.
     * <p>
     * Preserves original SQLState, ErrorCode, and Cause when wrapping {@link SQLException} types.
     * If {@code reason} is null or blank, the message is derived solely from {@code t}.
     * </p>
     *
     * @param <T> the type of the {@link Throwable} to wrap
     * @param reason an optional custom message prefix
     * @param t the {@link Throwable} to wrap
     * @return a {@link UcanaccessSQLException} instance
     */
    public static final <T extends Throwable> UcanaccessSQLException wrap(String reason, T t) {
        String r = reason == null || reason.isBlank() ? null : reason.trim();
        if (t instanceof UcanaccessSQLException) {
            UcanaccessSQLException ex = (UcanaccessSQLException) t;
            if (r == null) {
                return ex;
            }
            return new UcanaccessSQLException(r + ": " + ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex.getCause());
        } else if (t instanceof SQLException) {
            SQLException ex = (SQLException) t;
            return new UcanaccessSQLException(r == null ? ex.getMessage() : r + ": " + ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex.getCause());
        } else {
            return new UcanaccessSQLException(r == null ? t.getMessage() : r + ": " + t.getMessage(), t);
        }
    }

    public static final <T extends UcanaccessSQLException> void throwIf(BooleanSupplier condition, Supplier<T> exceptionSupplier) throws T {
        if (condition.getAsBoolean()) {
            throw exceptionSupplier.get();
        }
    }

}
