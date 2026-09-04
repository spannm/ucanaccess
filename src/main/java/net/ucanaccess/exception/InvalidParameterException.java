package net.ucanaccess.exception;

public class InvalidParameterException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidParameterException(Object parm, Object val) {
        super("Parameter " + parm + " invalid: " + val);
    }

}
