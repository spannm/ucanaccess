package net.ucanaccess.exception;

/**
 * Unspecific {@code Ucanaccess} run-time exception.
 */
public class UcanaccessRuntimeException extends RuntimeException {

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

    public static <T> T requireNonNull(T obj, String _message) {
        if (obj == null) {
            throw new UcanaccessRuntimeException(_message);
        }
        return obj;
    }

    /**
     * Convenience method to throw a {@code UcanaccessRuntimeException} with the specified error message.<br> Using this method rather than {@code throw new} avoids blocks in lambdas.
     *
     * @param _message exception message
     */
    public static void throwNow(String _message) {
        throw new UcanaccessRuntimeException(_message);
    }

    public static void throwNow(String _message, Throwable _cause) {
        throw new UcanaccessRuntimeException(_message, _cause);
    }

}
