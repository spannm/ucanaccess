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

public class DropTableCommand implements ICommand {
	private String execId;
	private String tableName;
	
	public DropTableCommand(String tableName, String execId) {
		super();
		this.tableName = tableName;
		this.execId = execId;
	}
	
	public String getExecId() {
		return execId;
	}
	
	
	
	public String getTableName() {
		return tableName;
	}

	public TYPES getType() {
		return TYPES.DDL;
	}
	
		
	public IFeedbackAction  persist() throws SQLException {
		try {
			Persist2Jet p2a = new Persist2Jet();
			p2a.dropTable(tableName);
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
		return null;
	}

	public IFeedbackAction rollback() throws SQLException {
		return null;
	}
}
