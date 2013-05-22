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
package net.ucanaccess.commands;

import java.io.IOException;
import java.sql.SQLException;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.triggers.AutoNumberManager;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;

public class InsertCommand implements ICommand {
	private Database dbIO;
	private String execId;
	private Object[] newRow;
	private Table table;
	private String tableName;
	
	public InsertCommand(String tableName, Database dbIO, Object[] newRow,
			String execId) {
		super();
		this.tableName = tableName;
		this.dbIO = dbIO;
		this.newRow = newRow;
		this.execId = execId;
	}
	
	public InsertCommand(Table table, Object[] newRow, String execId) {
		super();
		this.table = table;
		this.tableName = table.getName();
		this.newRow = newRow;
		this.execId = execId;
	}
	
	public String getExecId() {
		return execId;
	}
	
	public String getTableName() {
		return tableName;
	}
	
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
	
	public IFeedbackAction persist() throws SQLException {
		try {
			if (table == null)
				table = this.dbIO.getTable(this.tableName);
			Object[] memento = mementoRow();
			table.addRow(newRow);
			int j = 0;
			for (Column cl : table.getColumns()) {
				if (cl.isAutoNumber()
						&& cl.getAutoNumberGenerator().getType().equals(
								DataType.LONG) && !memento[j].equals(newRow[j])) {
					AutoNumberManager.reset(cl, (Integer) newRow[j]);
					return new AutoNumberAction(table, memento, newRow);
				}
				++j;
			}
			return null;
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public IFeedbackAction rollback() throws SQLException {
		if (this.table != null) {
			DeleteCommand dc = new DeleteCommand(
					this.table,
					new Persist2Jet().getRowPattern(this.newRow, this.table),
					this.execId);
			return dc.persist();
		} else
			// a drop table cleans all
			return null;
	}
}
