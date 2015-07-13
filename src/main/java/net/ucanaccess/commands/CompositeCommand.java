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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import net.ucanaccess.jdbc.UcanaccessSQLException;


import com.healthmarketscience.jackcess.Cursor;

public class CompositeCommand implements ICommand {
	private ArrayList<ICursorCommand> composite = new ArrayList<ICursorCommand>();
	private Map<String, Object> currentRow;
	private String execId;
	private IndexSelector indexSelector;
	private ArrayList<ICursorCommand> rollbackCache=new ArrayList<ICursorCommand>(); 
	
	public CompositeCommand() {
	}
	
	public boolean add(ICursorCommand c4io) {
		if (this.indexSelector == null) {
			this.indexSelector = c4io.getIndexSelector();
			this.execId = c4io.getExecId();
		}
		return composite.add(c4io);
	}
	
	
	
	public ArrayList<ICursorCommand> getComposite() {
		return composite;
	}

	public String getExecId() {
		return this.execId;
	}
	
	public String getTableName() {
		return composite.get(0).getTableName();
	}
	
	public TYPES getType() {
		return TYPES.COMPOSITE;
	}
	
	public boolean moveToNextRow(Cursor cur, Collection<String> columnNames)
			throws IOException {
		boolean hasNext = cur.moveToNextRow();
		if (hasNext) {
			this.currentRow = cur.getCurrentRow(columnNames);
		}
		return hasNext;
	}
	
	public IFeedbackAction  persist() throws SQLException {
		try {
			Cursor cur = indexSelector.getCursor();
			cur.beforeFirst();
			Collection<String> columnNames = composite.get(0).getRowPattern()
					.keySet();
			while (composite.size() > 0 && moveToNextRow(cur, columnNames)) {
				Iterator<ICursorCommand> it = composite.iterator();
				while (it.hasNext()) {
					ICursorCommand comm = it.next();
					if (comm.currentRowMatches(cur, this.currentRow)) {
						comm.persistCurrentRow(cur);
						it. remove();
						rollbackCache.add(comm);
						break;
					}
				}
			}
			return null;
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public IFeedbackAction rollback() throws SQLException {
		for(ICursorCommand ic:this.rollbackCache)
			ic.rollback();
		return null;
	}
}
