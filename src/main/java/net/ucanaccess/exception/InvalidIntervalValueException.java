package net.ucanaccess.exception;

public class InvalidIntervalValueException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidIntervalValueException(String _value) {
        super("Invalid interval value: " + _value);
    }
}
