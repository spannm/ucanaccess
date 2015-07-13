/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

*/
package net.ucanaccess.converters;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.TableImpl.RowState;

import com.healthmarketscience.jackcess.util.ErrorHandler;

public class UcanaccessTable implements Table{
	private Table wrapped;
	private String tableName;
	public UcanaccessTable(Table t, String tableName){
		this.wrapped=t;
		this.tableName=tableName;
	}
	public Object[] addRow(Object... row) throws IOException {
		return wrapped.addRow(row);
	}
	public <M extends Map<String, Object>> M addRowFromMap(M row)
			throws IOException {
		return wrapped.addRowFromMap(row);
	}
	public List<? extends Object[]> addRows(List<? extends Object[]> rows)
			throws IOException {
		return wrapped.addRows(rows);
	}
	public <M extends Map<String, Object>> List<M> addRowsFromMaps(List<M> rows)
			throws IOException {
		return wrapped.addRowsFromMaps(rows);
	}
	public Object[] asRow(Map<String, ?> rowMap) {
		return wrapped.asRow(rowMap);
	}
	public Object[] asUpdateRow(Map<String, ?> rowMap) {
		return wrapped.asUpdateRow(rowMap);
	}
	public Row deleteRow(Row row) throws IOException {
		return wrapped.deleteRow(row);
	}
	public Column getColumn(String name) {
		return wrapped.getColumn(name);
	}
	public int getColumnCount() {
		return wrapped.getColumnCount();
	}
	public List<? extends Column> getColumns() {
		return wrapped.getColumns();
	}
	public Database getDatabase() {
		return wrapped.getDatabase();
	}
	public Cursor getDefaultCursor() {
		return wrapped.getDefaultCursor();
	}
	public ErrorHandler getErrorHandler() {
		return wrapped.getErrorHandler();
	}
	public Index getForeignKeyIndex(Table otherTable) {
		return wrapped.getForeignKeyIndex(otherTable);
	}
	public Index getIndex(String name) {
		return wrapped.getIndex(name);
	}
	public List<? extends Index> getIndexes() {
		return wrapped.getIndexes();
	}
	public String getName() {
		return this.tableName;
	}
	public Row getNextRow() throws IOException {
		return wrapped.getNextRow();
	}
	public Index getPrimaryKeyIndex() {
		return wrapped.getPrimaryKeyIndex();
	}
	public PropertyMap getProperties() throws IOException {
		return wrapped.getProperties();
	}
	public int getRowCount() {
		return wrapped.getRowCount();
	}
	public boolean isHidden() {
		return wrapped.isHidden();
	}
	public boolean isSystem() {
		return wrapped.isSystem();
	}
	public Iterator<Row> iterator() {
		return wrapped.iterator();
	}
	public CursorBuilder newCursor() {
		return wrapped.newCursor();
	}
	public void reset() {
		wrapped.reset();
	}
	public void setErrorHandler(ErrorHandler newErrorHandler) {
		wrapped.setErrorHandler(newErrorHandler);
	}
	public Row updateRow(Row row) throws IOException {
		return wrapped.updateRow(row);
	}
	public boolean isAllowAutoNumberInsert() {
		return wrapped.isAllowAutoNumberInsert();
	}
	public void setAllowAutoNumberInsert(Boolean arg0) {
		wrapped.setAllowAutoNumberInsert(arg0);
	}
	public RowState createRowState() {
		// TODO Auto-generated method stub
		 return  ((TableImpl)wrapped).createRowState();
	}
	
	
}
