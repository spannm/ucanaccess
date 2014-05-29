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
import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;


import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Table;

public class DeleteCommand extends AbstractCursorCommand {
	private String execId;
	private IndexSelector indexSelector;
	private Map<String, Object> rowPattern;
	private Table table;
	
	
	public DeleteCommand(Table table, Map<String, Object> rowPattern, String execId) {
		super();
		this.indexSelector = new IndexSelector(table);
		this.rowPattern =rowPattern;
		this.execId = execId;
		this.table=table;
		
	}
	
	public String getExecId() {
		return execId;
	}

	public IndexSelector getIndexSelector() {
		return indexSelector;
	}
	
	public Map<String, Object> getRowPattern() {
		return rowPattern;
	}
	
	public String getTableName() {
		return table.getName();
	}
	
	public TYPES getType() {
		return TYPES.DELETE;
	}
	
	public IFeedbackAction  persist() throws SQLException {
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
	
	public void persistCurrentRow(Cursor cur) throws IOException {
		cur.deleteCurrentRow();
	}
	
	
	public IFeedbackAction rollback() throws SQLException {
		   InsertCommand ic = new InsertCommand(
					this.table,
					new Persist2Jet().getValues(this.rowPattern, this.table),
					this.execId);
			return ic.persist();
		
	}


}
