package net.ucanaccess.commands;

import io.github.spannm.jackcess.*;
import io.github.spannm.jackcess.Table.ColumnOrder;
import io.github.spannm.jackcess.complex.ComplexValueForeignKey;
import io.github.spannm.jackcess.impl.ColumnImpl;
import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.DBReferenceSingleton;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.triggers.AutoNumberManager;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InsertCommand implements ICommand {
    private final String tableName;
    private Database     dbIO;
    private Object[]     newRow;
    private final String execId;
    private Table        table;

    public InsertCommand(String tableName, Database dbIo, Object[] newRow, String execId) {
        this.tableName = tableName;
        dbIO = dbIo;
        this.newRow = newRow;
        this.execId = execId;

    }

    public InsertCommand(Table table, Object[] newRow, String execId) {
        this.table = table;
        tableName = table.getName();
        this.newRow = newRow;
        this.execId = execId;
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

    /**
     * Adds the new row to the given table.
     *
     * @return the table the row was added to and the id of the new row
     */
    private Map.Entry<Table, RowId> insertRow(Table tbl) throws IOException {
        // the element after the last column receives the id of the new row
        int numCols = tbl.getColumnCount();
        Object[] row = Arrays.copyOf(newRow, numCols + 1);
        row[numCols] = ColumnImpl.RETURN_ROW_ID;
        try {
            tbl.addRow(row);
        } catch (ConstraintViolationException ex) {
            List<? extends Column> lc = tbl.getColumns();
            boolean retry = false;
            for (Column col : lc) {
                if (col.isAutoNumber()) {
                    retry = true;
                    break;
                }
            }
            if (!retry) {
                throw ex;
            }
            Database db = tbl.getDatabase();
            File fl = db.getFile();
            DBReferenceSingleton dbsin = DBReferenceSingleton.getInstance();
            DBReference ref = dbsin.getReference(fl);
            ref.reloadDbIO();
            dbIO = ref.getDbIO();
            tbl = dbIO.getTable(tableName);
            tbl.addRow(row);
        }
        // the table fills in generated values such as autonumbers and column defaults
        System.arraycopy(row, 0, newRow, 0, Math.min(newRow.length, numCols));
        return new SimpleImmutableEntry<>(tbl, row[numCols] instanceof RowId ? (RowId) row[numCols] : null);
    }

    /**
     * Jackcess replaces {@code null} with the column default on insert. The row has already been inserted into HSQLDB,
     * which applied the defaults of omitted columns, so a remaining {@code null} was set explicitly and is restored.
     */
    private void restoreExplicitNulls(Table tbl, RowId rowId, Object[] memento, List<? extends Column> colList)
        throws IOException, SQLException {
        if (rowId == null) {
            return;
        }
        Cursor cursor = null;
        int j = 0;
        for (Column col : colList) {
            if (memento[j] == null && newRow[j] != null && !col.isAutoNumber() && col.getType() != DataType.BOOLEAN
                && col.getProperties().getValue(PropertyMap.DEFAULT_VALUE_PROP) != null && hasNativeDefault(col)) {
                if (cursor == null) {
                    cursor = CursorBuilder.createCursor(tbl);
                    cursor.findRow(rowId);
                }
                cursor.setCurrentRowValue(col, null);
                newRow[j] = null;
            }
            j++;
        }
    }

    /**
     * Only a native HSQLDB column default leaves an explicit {@code null} in the row. Defaults that UCanAccess could not
     * translate are left to Jackcess.
     */
    private static boolean hasNativeDefault(Column col) throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        return conn != null && new Metadata(conn.getHSQLDBConnection()).hasNativeColumnDefault(col.getTable().getName(), col.getName());
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

            Map.Entry<Table, RowId> inserted = insertRow(table);
            restoreExplicitNulls(inserted.getKey(), inserted.getValue(), memento, colList);
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
        } catch (IOException ex) {
            throw new UcanaccessSQLException(ex);
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
