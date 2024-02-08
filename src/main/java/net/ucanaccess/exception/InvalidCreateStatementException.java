package net.ucanaccess.exception;

public class InvalidCreateStatementException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidCreateStatementException(String _sql) {
        super("Invalid create statement: " + _sql);
    }
}
