package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;

/**
 * Abstract base class for commands that use a Jackcess {@link Cursor} to locate and manipulate rows.
 * <p>
 * Provides common functionality for matching the current row against a provided pattern
 * and handling autoincrement column updates.
 */
public abstract class AbstractCursorCommand implements ICursorCommand {
    private static final Logger LOGGER = System.getLogger(AbstractCursorCommand.class.getName());

    @Override
    public boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow) {
        Map<String, Object> rowPattern = getRowPattern();
        if (rowPattern.size() != currentRow.size()) {
            LOGGER.log(Level.DEBUG, "Row size mismatch: expected {0}, found {1}",
                new Object[] {rowPattern.size(), currentRow.size()});
            return false;
        }

        for (Map.Entry<String, Object> e : currentRow.entrySet()) {
            String columnName = e.getKey();
            Object expectedValue = rowPattern.get(columnName);
            Object actualValue = e.getValue();

            if (!cur.getColumnMatcher().matches(cur.getTable(), columnName, expectedValue, actualValue)) {
                LOGGER.log(Level.WARNING, "Column value mismatch in table ''{0}'', column ''{1}'', expected ''{2}'', actual ''{3}''",
                    new Object[] {cur.getTable().getName(), columnName, expectedValue, actualValue});
                return false;
            }
        }
        return true;
    }

    /**
     * Updates the row pattern with autoincrement values.
     *
     * @param map map containing the autoincrement values
     */
    public void replaceAutoincrement(Map<String, Object> map) {
        getRowPattern().putAll(map);
    }

    @Override
    public String toString() {
        return toIdentString();
    }

}
