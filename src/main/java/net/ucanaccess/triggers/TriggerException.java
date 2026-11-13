package net.ucanaccess.triggers;

public class TriggerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TriggerException(String msg) {
        super(msg);
    }

    /**
     * Creates a trigger exception that keeps the exception it was caused by.
     *
     * @param msg   the detail message
     * @param cause the original exception
     */
    public TriggerException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
