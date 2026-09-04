package net.ucanaccess.exception;

public class TableNotFoundException extends UcanaccessSQLException {
    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String name) {
        super("Table " + name + " not found");
    }

}
