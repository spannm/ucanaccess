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
