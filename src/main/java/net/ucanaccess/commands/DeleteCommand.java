package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class DeleteCommand extends AbstractCursorCommand {
    private String              execId;
    private IndexSelector       indexSelector;
    private Map<String, Object> rowPattern;
    private Table               table;

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
        InsertCommand ic =
                new InsertCommand(table, new Persist2Jet().getValues(rowPattern, table), execId);
        return ic.persist();

    }

}
