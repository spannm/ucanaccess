package net.ucanaccess.jdbc;

import net.ucanaccess.log.Logger;
import net.ucanaccess.log.LoggerMessageEnum;

/**
 * Unspecific {@code Ucanaccess} run-time exception.
 */
public final class UcanaccessRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UcanaccessRuntimeException(String _message) {
        this(_message, null);
    }

    public UcanaccessRuntimeException(Throwable _cause) {
        this(null, _cause);
    }

    public UcanaccessRuntimeException(String _message, Throwable _cause) {
        super(_message, _cause);
    }

    public static UcanaccessRuntimeException featureNotSupported() {
        return new UcanaccessRuntimeException(Logger.getMessage(LoggerMessageEnum.NOT_SUPPORTED));
    }

}
