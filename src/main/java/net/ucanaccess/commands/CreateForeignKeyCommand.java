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

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

public class CreateForeignKeyCommand implements ICommand {
	
	private String tableName;
	private String referencedTable;
	private String execId;


    public CreateForeignKeyCommand(String tableName, String referencedTable,
			String execId) {
		super();
		this.tableName = tableName;
		this.referencedTable = referencedTable;
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
	
	
	public IFeedbackAction persist() throws SQLException {
		try {
			Persist2Jet p2a = new Persist2Jet();
			p2a.createForeignKey(this.tableName,this.referencedTable);
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
		return null;
	}



	
	public IFeedbackAction rollback() throws SQLException {
		return null;
	}
}
