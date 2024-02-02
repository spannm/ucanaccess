package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class CompositeCommand implements ICommand {
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
}
