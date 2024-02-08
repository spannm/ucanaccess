package net.ucanaccess.exception;

public class AuthenticationException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public AuthenticationException() {
        super("Authentication failed");
    }
}
