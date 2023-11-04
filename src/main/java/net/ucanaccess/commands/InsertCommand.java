package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.Table.ColumnOrder;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.DBReferenceSingleton;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.triggers.AutoNumberManager;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class InsertCommand implements ICommand {
    private Database dbIO;
    private String   execId;
    private Object[] newRow;
    private Table    table;
    private String   tableName;

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
    public TYPES getType() {
        return TYPES.INSERT;
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

    public void insertRow(Table _table, Object[] _row) throws IOException {
        try {
            _table.addRow(newRow);
        } catch (ConstraintViolationException _ex) {
            List<? extends Column> lc = _table.getColumns();
            boolean retry = false;
            for (Column cl : lc) {
                if (cl.isAutoNumber()) {
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
            List<? extends Column> lc = table.getColumns();
            if (table.getDatabase().getColumnOrder().equals(ColumnOrder.DISPLAY)) {
                Object[] newRowReorded = new Object[newRow.length];
                Column[] cllReorded = new Column[newRow.length];
                for (Column cli : table.getColumns()) {
                    newRowReorded[cli.getColumnIndex()] = newRow[j];
                    memento[cli.getColumnIndex()] = newRow[j];
                    cllReorded[cli.getColumnIndex()] = cli;
                    j++;
                }
                newRow = newRowReorded;
                lc = Arrays.asList(cllReorded);
            }

            insertRow(table, newRow);
            j = 0;
            for (Column cli : lc) {
                ColumnImpl cl = (ColumnImpl) cli;
                if (cl.isAutoNumber() && !memento[j].equals(newRow[j])
                        && !cl.getAutoNumberGenerator().getType().equals(DataType.COMPLEX_TYPE)) {

                    if (cl.getAutoNumberGenerator().getType().equals(DataType.LONG)) {
                        AutoNumberManager.reset(cl, (Integer) newRow[j]);
                    }
                    ana = new AutoNumberAction(table, memento, newRow);
                }

                if (cl.getType() == DataType.COMPLEX_TYPE) {
                    ComplexValueForeignKey rowFk = (ComplexValueForeignKey) cl.getRowValue(newRow);
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
