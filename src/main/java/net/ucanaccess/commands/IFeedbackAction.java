package net.ucanaccess.commands;

import java.sql.SQLException;

/**
 * Interface for actions to be executed as feedback after a command has been persisted.
 * <p>
 * This is primarily used to synchronize the state of the HSQLDB mirror with the
 * results of the Jackcess persistence layer (e.g., updating autoincrement values
 * or handling complex type identifiers).
 */
public interface IFeedbackAction {

    /**
     * Executes the feedback action on the specified command.
     * <p>
     * This method is called after the physical persistence to the Access file
     * has succeeded, allowing for final adjustments to the command's state.
     *
     * @param toChange the command instance that may be modified by this action
     * @throws SQLException if an error occurs during the execution of the feedback action
     */
    void doAction(ICommand toChange) throws SQLException;

}
