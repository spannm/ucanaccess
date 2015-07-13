/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
