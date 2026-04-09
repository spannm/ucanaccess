package net.ucanaccess.commands;

import java.sql.SQLException;

/**
 * Represents a database command that can be persisted to the Access file or rolled back.
 * <p>
 * This interface defines the contract for all data manipulation and definition operations
 * within the UCanAccess command execution pipeline.
 */
public interface ICommand {

    /**
     * Enumeration of supported command types.
     */
    enum CommandType {
        COMPOSITE,
        DDL,
        DELETE,
        INSERT,
        UPDATE
    }

    /**
     * Returns the unique execution identifier.
     *
     * @return the execution id
     */
    String getExecId();

    /**
     * Returns the name of the table affected by this command.
     *
     * @return the table name
     */
    String getTableName();

    /**
     * Returns the type of this command.
     *
     * @return the command type
     */
    CommandType getType();

    /**
     * Persists the changes to the underlying Access database file.
     *
     * @return a feedback action to be executed after persistence
     * @throws SQLException if a database error occurs during persistence
     */
    IFeedbackAction persist() throws SQLException;

    /**
     * Reverts the changes made by this command.
     *
     * @return a feedback action to be executed after rollback
     * @throws SQLException if a database error occurs during rollback
     */
    IFeedbackAction rollback() throws SQLException;

    /**
     * Returns a string representation of the command.
     * <p>
     * Follows the format: ClassName[execId=id, type=type, table=tableName]
     *
     * @return string representation
     */
    default String toIdentString() {
        return String.format("%s[execId=%s, type=%s, table=%s]",
            getClass().getSimpleName(), getExecId(), getType(), getTableName());
    }

}

