package net.ucanaccess.exception;

public class InvalidFunctionParameterException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidFunctionParameterException(String function, Object parm) {
        super("Invalid parameter for function " + function + ": " + parm);
    }

}
