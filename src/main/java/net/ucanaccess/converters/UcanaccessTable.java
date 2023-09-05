package net.ucanaccess.converters;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.TableImpl.RowState;
import com.healthmarketscience.jackcess.util.ErrorHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UcanaccessTable implements Table {
    private Table  wrapped;
    private String tableName;

    public UcanaccessTable(Table _table, String _tableName) {
        wrapped = _table;
        tableName = _tableName;
    }

    @Override
    public Object[] addRow(Object... row) throws IOException {
        return wrapped.addRow(row);
    }

    @Override
    public <M extends Map<String, Object>> M addRowFromMap(M row) throws IOException {
        return wrapped.addRowFromMap(row);
    }

    @Override
    public List<? extends Object[]> addRows(List<? extends Object[]> rows) throws IOException {
        return wrapped.addRows(rows);
    }

    @Override
    public <M extends Map<String, Object>> List<M> addRowsFromMaps(List<M> rows) throws IOException {
        return wrapped.addRowsFromMaps(rows);
    }

    @Override
    public Object[] asRow(Map<String, ?> rowMap) {
        return wrapped.asRow(rowMap);
    }

    @Override
    public Object[] asUpdateRow(Map<String, ?> rowMap) {
        return wrapped.asUpdateRow(rowMap);
    }

    @Override
    public Row deleteRow(Row row) throws IOException {
        return wrapped.deleteRow(row);
    }

    @Override
    public Column getColumn(String name) {
        return wrapped.getColumn(name);
    }

    @Override
    public int getColumnCount() {
        return wrapped.getColumnCount();
    }

    @Override
    public List<? extends Column> getColumns() {
        return wrapped.getColumns();
    }

    @Override
    public Database getDatabase() {
        return wrapped.getDatabase();
    }

    @Override
    public Cursor getDefaultCursor() {
        return wrapped.getDefaultCursor();
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return wrapped.getErrorHandler();
    }

    @Override
    public Index getForeignKeyIndex(Table otherTable) {
        return wrapped.getForeignKeyIndex(otherTable);
    }

    @Override
    public Index getIndex(String name) {
        return wrapped.getIndex(name);
    }

    @Override
    public List<? extends Index> getIndexes() {
        return wrapped.getIndexes();
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public Row getNextRow() throws IOException {
        return wrapped.getNextRow();
    }

    @Override
    public Index getPrimaryKeyIndex() {
        return wrapped.getPrimaryKeyIndex();
    }

    @Override
    public PropertyMap getProperties() throws IOException {
        return wrapped.getProperties();
    }

    @Override
    public int getRowCount() {
        return wrapped.getRowCount();
    }

    @Override
    public boolean isHidden() {
        return wrapped.isHidden();
    }

    @Override
    public boolean isSystem() {
        return wrapped.isSystem();
    }

    @Override
    public Iterator<Row> iterator() {
        return wrapped.iterator();
    }

    @Override
    public CursorBuilder newCursor() {
        return wrapped.newCursor();
    }

    @Override
    public void reset() {
        wrapped.reset();
    }

    @Override
    public void setErrorHandler(ErrorHandler newErrorHandler) {
        wrapped.setErrorHandler(newErrorHandler);
    }

    @Override
    public Row updateRow(Row row) throws IOException {
        return wrapped.updateRow(row);
    }

    @Override
    public boolean isAllowAutoNumberInsert() {
        return wrapped.isAllowAutoNumberInsert();
    }

    @Override
    public void setAllowAutoNumberInsert(Boolean arg0) {
        wrapped.setAllowAutoNumberInsert(arg0);
    }

    public RowState createRowState() {
        // TODO Auto-generated method stub
        return ((TableImpl) wrapped).createRowState();
    }

}
