package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.SQLException;
import java.util.*;

public class CompositeCommand implements ICommand {
    private final Logger               logger        = System.getLogger(getClass().getName());

    private final List<ICursorCommand> composite     = new ArrayList<>();
    private Map<String, Object>        currentRow;
    private String                     execId;
    private IndexSelector              indexSelector;
    private final List<ICursorCommand> rollbackCache = new ArrayList<>();

    public boolean add(ICursorCommand c4io) {
        if (indexSelector == null) {
            indexSelector = c4io.getIndexSelector();
            execId = c4io.getExecId();
        }
        return composite.add(c4io);
    }

    public List<ICursorCommand> getComposite() {
        return composite;
    }

    @Override
    public String getExecId() {
        return execId;
    }

    @Override
    public String getTableName() {
        return composite.get(0).getTableName();
    }

    @Override
    public CommandType getType() {
        return CommandType.COMPOSITE;
    }

    public boolean moveToNextRow(Cursor cur, Set<String> columnNames) throws IOException {
        boolean hasNext = cur.moveToNextRow();
        if (hasNext) {
            currentRow = cur.getCurrentRow(columnNames);
        }
        return hasNext;
    }

    /**
     * Persists the composite commands by iterating over the table cursor and matching rows.
     * <p>
     * For each row in the table, it checks if any command in the composite list matches
     * the current row's data pattern. If a match is found, the command is executed
     * against the current row.
     *
     * @return a composite feedback action containing all individual feedback actions
     * @throws SQLException if a critical persistence mismatch occurs or database access fails
     */
    @Override
    public IFeedbackAction persist() throws SQLException {
        return Try.catching(() -> {
            CompositeFeedbackAction cfa = new CompositeFeedbackAction();
            Cursor cur = indexSelector.getCursor();
            cur.beforeFirst();
            Set<String> columnNames = composite.get(0).getRowPattern().keySet();
            while (!composite.isEmpty() && moveToNextRow(cur, columnNames)) {
                Iterator<ICursorCommand> it = composite.iterator();
                while (it.hasNext()) {
                    ICursorCommand comm = it.next();
                    if (comm.currentRowMatches(cur, currentRow)) {
                        cfa.add(comm.persistCurrentRow(cur));
                        it.remove();
                        rollbackCache.add(comm);
                        break;
                    }
                }
            }

            // log warning for commands that could not be synchronized with the physical file
            if (!composite.isEmpty()) {
                logger.log(Level.WARNING,
                    "Persistence mismatch for table {0}: {1} commands (Type: {2}) could not be matched in the Access file",
                    new Object[] {getTableName(), composite.size(), composite.get(0).getType()});
            }

            return cfa;
        }).orThrow(UcanaccessSQLException::new);
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        for (ICursorCommand ic : rollbackCache) {
            ic.rollback();
        }
        return null;
    }

    @Override
    public String toString() {
        return toIdentString();
    }

}
