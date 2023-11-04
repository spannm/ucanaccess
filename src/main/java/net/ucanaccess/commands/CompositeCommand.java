package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class CompositeCommand implements ICommand {
    private List<ICursorCommand> composite     = new ArrayList<>();
    private Map<String, Object>  currentRow;
    private String               execId;
    private IndexSelector        indexSelector;
    private List<ICursorCommand> rollbackCache = new ArrayList<>();

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
    public TYPES getType() {
        return TYPES.COMPOSITE;
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
        try {
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
        } catch (IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        for (ICursorCommand ic : rollbackCache) {
            ic.rollback();
        }
        return null;
    }
}
