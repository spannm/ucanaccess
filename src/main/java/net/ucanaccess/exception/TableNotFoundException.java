package net.ucanaccess.exception;

public class TableNotFoundException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String _name) {
        super("Table " + _name + " not found");
    }

}
