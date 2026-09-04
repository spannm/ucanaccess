package net.ucanaccess.exception;

/**
 * Unspecific {@code Ucanaccess} run-time exception.
 */
public class UcanaccessRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UcanaccessRuntimeException(String message) {
        this(message, null);
    }

    public UcanaccessRuntimeException(Throwable cause) {
        this(null, cause);
    }

    public UcanaccessRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static <T> T requireNonNull(T obj, String message) {
        if (obj == null) {
            throw new UcanaccessRuntimeException(message);
        }
        return obj;
    }

    /**
     * Convenience method to throw a {@code UcanaccessRuntimeException} with the specified error message.<br> Using this method rather than {@code throw new} avoids blocks in lambdas.
     *
     * @param message exception message
     */
    public static void throwNow(String message) {
        throw new UcanaccessRuntimeException(message);
    }

    public static void throwNow(String message, Throwable cause) {
        throw new UcanaccessRuntimeException(message, cause);
    }

}
