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
package net.ucanaccess.triggers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.converters.SQLConverter;
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
		}
		if(t==null){
			Database db=conn.getDbIO();
			for (String cand:db.getTableNames()){
				if(SQLConverter.escapeIdentifier(cand).equals(tableName)){
					return db .getTable(cand);
				}
				
			}
		}
		return t;
	}
	
}
