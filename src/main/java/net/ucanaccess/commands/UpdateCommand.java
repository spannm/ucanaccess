package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Table.ColumnOrder;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateCommand extends AbstractCursorCommand {
    private List<Column>           blobColumns;
    private String                 execId;
    private IndexSelector          indexSelector;
    private Object[]               modifiedRow;
    private Map<String, Object>    rowPattern;
    private Table                  table;
    private List<? extends Column> tableColumns;
    private boolean                isRollbacking;

    public UpdateCommand(Table _table, Map<String, Object> _map, Object[] _modifiedRow, String _execId) {
        tableColumns = _table.getColumns();
        indexSelector = new IndexSelector(_table);
        rowPattern = _map;
        modifiedRow = _modifiedRow;
        execId = _execId;
        checkBlob(_modifiedRow);
        table = _table;
    }

    private void checkBlob(Object[] newRow2) {
        for (int i = 0; i < newRow2.length; i++) {
            if (newRow2[i] instanceof org.hsqldb.types.BlobData) {
                if (blobColumns == null) {
                    blobColumns = new ArrayList<>();
                }
                blobColumns.add(tableColumns.get(i));
            }
        }
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
        return CommandType.UPDATE;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        Try.catching(() -> {
            Cursor cur = indexSelector.getCursor();
            if (cur.findNextRow(rowPattern)) {
                if (blobColumns != null) {
                    for (Column col : blobColumns) {
                        Object val = cur.getCurrentRowValue(col);
                        modifiedRow[tableColumns.indexOf(col)] = val;
                    }
                }
                updateComplex(cur);
                persist(cur);
            }
        }).orThrow(UcanaccessSQLException::new);
        return new BlobAction(table, modifiedRow);
    }

    @Override
    public IFeedbackAction persistCurrentRow(Cursor cur) throws IOException {
        if (blobColumns != null) {
            for (Column col : blobColumns) {
                Object val = cur.getCurrentRowValue(col);
                modifiedRow[tableColumns.indexOf(col)] = val;

            }
        }
        updateComplex(cur);
        persist(cur);
        return new BlobAction(table, modifiedRow);
    }

    private void updateComplex(Cursor cur) throws IOException {
        int j = 0;

        for (Column col : tableColumns) {
            if (col.getType() == DataType.COMPLEX_TYPE) {
                ComplexValueForeignKey rowFk = (ComplexValueForeignKey) col.getRowValue(cur.getCurrentRow());

                if (modifiedRow[j] instanceof Attachment[]) {
                    rowFk.deleteAllValues();
                    Attachment[] atcs = (Attachment[]) modifiedRow[j];
                    for (Attachment atc : atcs) {
                        rowFk.addAttachment(atc.getUrl(), atc.getName(), atc.getType(), atc.getData(),
                                atc.getTimeStamp(), atc.getFlags());

                    }
                } else if (modifiedRow[j] instanceof SingleValue[]) {
                    rowFk.deleteAllValues();
                    SingleValue[] vs = (SingleValue[]) modifiedRow[j];
                    for (SingleValue v : vs) {
                        rowFk.addMultiValue(v.getValue());
                    }

                } else if (modifiedRow[j] instanceof Version[]) {

                    Version[] vs = (Version[]) modifiedRow[j];
                    if (vs.length > 0) {
                        Version v = vs[0];
                        List<com.healthmarketscience.jackcess.complex.Version> oldV = rowFk.getVersions();
                        String vn = v.getValue();
                        String vo = !oldV.isEmpty() ? oldV.get(0).getValue() : null;
                        LocalDateTime upTime = isRollbacking ? LocalDateTime.now() : v.getModifiedDate();

                        if (vn != null && vo == null || vo != null && vn == null
                                || vo != null && vn != null && !vo.equals(vn)) {
                            rowFk.addVersion(vn, upTime);
                        }
                    }

                }
            }
            j++;
        }
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        Persist2Jet p2a = new Persist2Jet();

        UpdateCommand urev = new UpdateCommand(table, p2a.getRowPattern(modifiedRow, table),
                p2a.getValues(getRowPattern(), table), execId);
        urev.isRollbacking = true;
        return urev.persist();
    }

    private void persist(Cursor cur) throws IOException {
        Object[] mr = modifiedRow;
        if (table.getDatabase().getColumnOrder().equals(ColumnOrder.DISPLAY)) {
            Object[] newRowReorded = new Object[modifiedRow.length];
            int j = 0;
            for (Column col : table.getColumns()) {
                newRowReorded[col.getColumnIndex()] = modifiedRow[j];
                j++;
            }
            mr = newRowReorded;
        }
        cur.updateCurrentRow(mr);
    }

}
