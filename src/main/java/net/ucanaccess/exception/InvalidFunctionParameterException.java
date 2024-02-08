package net.ucanaccess.exception;

public class InvalidFunctionParameterException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidFunctionParameterException(String _function, Object _parm) {
        super("Invalid parameter for function " + _function + ": " + _parm);
    }

}
