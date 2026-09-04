package net.ucanaccess.exception;

public class UnsupportedTypeException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public UnsupportedTypeException(String type) {
        super("Type not supported: " + type);
    }

}
