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

public class DropForeignKeyCommand implements ICommand {

	private String	execId;
	private String	relationshipName;

	public DropForeignKeyCommand(String execId, String relationshipName) {
		super();
		this.execId = execId;
		this.relationshipName = relationshipName;
	}

	public String getRelationshipName() {
		return relationshipName;
	}

	public String getExecId() {
		return execId;
	}

	public String getTableName() {
		throw new UnsupportedOperationException("The getTableName method is not applicable to this object.");
	}

	public TYPES getType() {
		return TYPES.DDL;
	}

	public IFeedbackAction persist() throws SQLException {
		try {
			Persist2Jet p2a = new Persist2Jet();
			p2a.dropForeignKey(this.relationshipName);
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
		return null;
	}

	public IFeedbackAction rollback() throws SQLException {
		return null;
	}
}
