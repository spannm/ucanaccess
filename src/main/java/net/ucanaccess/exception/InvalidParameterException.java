package net.ucanaccess.exception;

public class InvalidParameterException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public InvalidParameterException(Object _parm, Object _val) {
        super("Parameter " + _parm + " invalid: " + _val);
    }

}
