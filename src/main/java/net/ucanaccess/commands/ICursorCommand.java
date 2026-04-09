package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;

import java.io.IOException;
import java.util.Map;

/**
 * Extension of {@link ICommand} for commands that require a Jackcess {@link Cursor} to identify or manipulate records.
 * <p>
 * This interface provides methods to match patterns against the current database state
 * and to persist changes specifically at the current cursor position.
 */
public interface ICursorCommand extends ICommand {

    /**
     * Verifies if the current row at the cursor position matches the provided data pattern.
     *
     * @param cur the active database cursor
     * @param currentRow the data of the row to compare
     * @return true if the row matches the pattern, false otherwise
     */
    boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow);

    /**
     * Returns the {@link IndexSelector} used to obtain the appropriate cursor for this command.
     *
     * @return the index selector
     */
    IndexSelector getIndexSelector();

    /**
     * Returns the pattern of column names and values used to identify the target row.
     *
     * @return the row pattern map
     */
    Map<String, Object> getRowPattern();

    /**
     * Persists the command's changes to the row currently pointed to by the cursor.
     * <p>
     * This is typically used by {@link CompositeCommand} to perform batch-like updates
     * while iterating over a table.
     *
     * @param cur the cursor positioned at the target row
     * @return a feedback action to be executed after persistence
     * @throws IOException if an I/O error occurs during the Jackcess operation
     */
    IFeedbackAction persistCurrentRow(Cursor cur) throws IOException;
}
