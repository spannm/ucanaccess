package net.ucanaccess.commands;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Table;

public class DeleteCommand extends AbstractCursorCommand {
    private String              execId;
    private IndexSelector       indexSelector;
    private Map<String, Object> rowPattern;
    private Table               table;

    public DeleteCommand(Table _table, Map<String, Object> _rowPattern, String _execId) {
        this.indexSelector = new IndexSelector(_table);
        this.rowPattern = _rowPattern;
        this.execId = _execId;
        this.table = _table;
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
    public TYPES getType() {
        return TYPES.DELETE;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            Cursor cur = indexSelector.getCursor();
            if (cur.findNextRow(rowPattern)) {
                cur.deleteCurrentRow();
            }
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
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
                new InsertCommand(this.table, new Persist2Jet().getValues(this.rowPattern, this.table), this.execId);
        return ic.persist();

    }

}
