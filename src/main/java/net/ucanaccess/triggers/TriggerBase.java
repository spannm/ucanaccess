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
package net.ucanaccess.triggers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.converters.UcanaccessTable;
import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;

public abstract class TriggerBase implements org.hsqldb.Trigger {
	public static final Persist2Jet p2a = new Persist2Jet();
	public static final String ESCAPE_PREFIX = "X";
	public void convertRowTypes(Object[] values, Table table)
			throws SQLException {
		p2a.convertRowTypes(values, table);
	}
	
	protected Map<String, Object> getRowPattern(Object[] values, Table t)
			throws SQLException {
		return p2a.getRowPattern(values, t);
	}
	protected Table getTable(String tableName,UcanaccessConnection conn ) throws IOException{
		Table t=conn.getDbIO().getTable(tableName);
		if(t==null&&tableName.startsWith(ESCAPE_PREFIX )&&
				SQLConverter.isXescaped(tableName.substring(1))){
			t=conn.getDbIO().getTable(tableName.substring(1));
			if(t!=null){
				return new  UcanaccessTable(t,tableName.substring(1));
			}
		}
		if(t==null){
			Database db=conn.getDbIO();
			for (String cand:db.getTableNames()){
				if(	SQLConverter.preEscapingIdentifier(cand).equals(tableName)||
						SQLConverter.escapeIdentifier(cand).equals(tableName)){
					return new UcanaccessTable(db.getTable(cand),cand);
				}
				
			}
		}
		 return new  UcanaccessTable(t,tableName);
	}
	
}
