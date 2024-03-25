package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;
import io.github.spannm.jackcess.Table;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class DeleteCommand extends AbstractCursorCommand {
    private final String              execId;
    private final IndexSelector       indexSelector;
    private final Map<String, Object> rowPattern;
    private final Table               table;

    public DeleteCommand(Table _table, Map<String, Object> _rowPattern, String _execId) {
        indexSelector = new IndexSelector(_table);
        rowPattern = _rowPattern;
        execId = _execId;
        table = _table;
    }

    @Override
    public String getExecId() {
        return execId;
    }

    @Override
    public IndexSelector getIndexSelector() {
        return indexSelector;
    }

    @Override
    public Map<String, Object> getRowPattern() {
        return rowPattern;
    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    @Override
    public CommandType getType() {
        return CommandType.DELETE;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> {
            Cursor cur = indexSelector.getCursor();
            if (cur.findNextRow(rowPattern)) {
                cur.deleteCurrentRow();
            }
        }).orThrow(UcanaccessSQLException::new);
        return null;
    }

    @Override
    public CompositeFeedbackAction persistCurrentRow(Cursor cur) throws IOException {
        cur.deleteCurrentRow();
        return null;
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        InsertCommand ic = new InsertCommand(table, new Persist2Jet().getValues(rowPattern, table), execId);
        return ic.persist();
    }

}
