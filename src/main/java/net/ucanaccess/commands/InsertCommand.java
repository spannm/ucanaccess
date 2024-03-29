package net.ucanaccess.commands;

import io.github.spannm.jackcess.*;
import io.github.spannm.jackcess.Table.ColumnOrder;
import io.github.spannm.jackcess.complex.ComplexValueForeignKey;
import io.github.spannm.jackcess.impl.ColumnImpl;
import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.DBReferenceSingleton;
import net.ucanaccess.triggers.AutoNumberManager;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class InsertCommand implements ICommand {
    private final String tableName;
    private Database     dbIO;
    private Object[]     newRow;
    private final String execId;
    private Table        table;

    public InsertCommand(String _tableName, Database _dbIo, Object[] _newRow, String _execId) {
        tableName = _tableName;
        dbIO = _dbIo;
        newRow = _newRow;
        execId = _execId;

    }

    public InsertCommand(Table _table, Object[] _newRow, String _execId) {
        table = _table;
        tableName = _table.getName();
        newRow = _newRow;
        execId = _execId;
    }

    @Override
    public String getExecId() {
        return execId;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public CommandType getType() {
        return CommandType.INSERT;
    }

    private Object[] mementoRow() {
        Object[] clone = new Object[newRow.length];
        int i = 0;
        for (Object obj : newRow) {
            clone[i] = obj;
            ++i;
        }
        return clone;
    }

    private void initComplex() {
        for (int i = 0; i < newRow.length; ++i) {
            if (newRow[i] instanceof ComplexBase) {
                newRow[i] = Column.AUTO_NUMBER;
            }
        }
    }

    public void insertRow(Table _table) throws IOException {
        try {
            _table.addRow(newRow);
        } catch (ConstraintViolationException _ex) {
            List<? extends Column> lc = _table.getColumns();
            boolean retry = false;
            for (Column col : lc) {
                if (col.isAutoNumber()) {
                    retry = true;
                    break;
                }
            }
            if (!retry) {
                throw _ex;
            }
            Database db = _table.getDatabase();
            File fl = db.getFile();
            DBReferenceSingleton dbsin = DBReferenceSingleton.getInstance();
            DBReference ref = dbsin.getReference(fl);
            ref.reloadDbIO();
            dbIO = ref.getDbIO();
            _table = dbIO.getTable(tableName);
            _table.addRow(newRow);
        }
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            AutoNumberAction ana = null;
            if (table == null) {
                table = dbIO.getTable(tableName);
            }
            Object[] memento = mementoRow();
            initComplex();
            int j = 0;
            List<? extends Column> colList = table.getColumns();
            if (table.getDatabase().getColumnOrder().equals(ColumnOrder.DISPLAY)) {
                Object[] newRowReorded = new Object[newRow.length];
                Column[] cllReorded = new Column[newRow.length];
                for (Column col : table.getColumns()) {
                    newRowReorded[col.getColumnIndex()] = newRow[j];
                    memento[col.getColumnIndex()] = newRow[j];
                    cllReorded[col.getColumnIndex()] = col;
                    j++;
                }
                newRow = newRowReorded;
                colList = Arrays.asList(cllReorded);
            }

            insertRow(table);
            j = 0;
            for (Column col : colList) {
                ColumnImpl colImpl = (ColumnImpl) col;
                if (colImpl.isAutoNumber() && !memento[j].equals(newRow[j])
                        && !colImpl.getAutoNumberGenerator().getType().equals(DataType.COMPLEX_TYPE)) {

                    if (colImpl.getAutoNumberGenerator().getType().equals(DataType.LONG)) {
                        AutoNumberManager.reset(colImpl, (Integer) newRow[j]);
                    }
                    ana = new AutoNumberAction(table, memento, newRow);
                }

                if (colImpl.getType() == DataType.COMPLEX_TYPE) {
                    ComplexValueForeignKey rowFk = (ComplexValueForeignKey) colImpl.getRowValue(newRow);
                    if (memento[j] instanceof Attachment[]) {
                        Attachment[] atcs = (Attachment[]) memento[j];
                        for (Attachment atc : atcs) {
                            rowFk.addAttachment(atc.getUrl(), atc.getName(), atc.getType(), atc.getData(),
                                    atc.getTimeStamp(), atc.getFlags());

                        }
                    } else if (memento[j] instanceof SingleValue[]) {
                        SingleValue[] vs = (SingleValue[]) memento[j];
                        for (SingleValue v : vs) {
                            rowFk.addMultiValue(v.getValue());
                        }

                    } else if (memento[j] instanceof Version[]) {
                        Version[] vs = (Version[]) memento[j];
                        for (Version v : vs) {
                            rowFk.addVersion(v.getValue(), v.getModifiedDate());
                        }
                    }
                }
                ++j;
            }
            BlobAction ba = new BlobAction(table, newRow);
            ba.doAction(this);
            return ana;
        } catch (IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        if (table != null) {
            DeleteCommand dc = new DeleteCommand(table, new Persist2Jet().getRowPattern(newRow, table),
                    execId);
            return dc.persist();
        } else {
            // a drop table cleans all
            return null;
        }
    }
}
