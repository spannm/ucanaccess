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
import java.util.Arrays;

import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

public class AddColumnCommand implements ICommand {
	private String execId;
	private String tableName;
	private String[] types;
	private String[] defaults;
	private Boolean[] notNulls;
	private Map<String,String> columnMap;
	
	
	public AddColumnCommand(String  tableName, String execId2,Map<String,String> columnMap,
			String[] types, String[] defaults, Boolean[] notNulls) {
		this.tableName = tableName;
		this.types =types;
		this.defaults=defaults;
		this.notNulls=notNulls;
		this.columnMap=columnMap;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnMap == null) ? 0 : columnMap.hashCode());
		result = prime * result + Arrays.hashCode(defaults);
		result = prime * result + ((execId == null) ? 0 : execId.hashCode());
		result = prime * result + Arrays.hashCode(notNulls);
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + Arrays.hashCode(types);
		return result;
	}













	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AddColumnCommand other = (AddColumnCommand) obj;
		if (columnMap == null) {
			if (other.columnMap != null)
				return false;
		} else if (!columnMap.equals(other.columnMap))
			return false;
		if (!Arrays.equals(defaults, other.defaults))
			return false;
		if (execId == null) {
			if (other.execId != null)
				return false;
		} else if (!execId.equals(other.execId))
			return false;
		if (!Arrays.equals(notNulls, other.notNulls))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (!Arrays.equals(types, other.types))
			return false;
		return true;
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
			p2a.addColumn(tableName,columnMap, types,defaults,notNulls);
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
		return null;
	}



	public IFeedbackAction rollback() throws SQLException {
		return null;
	}
}
