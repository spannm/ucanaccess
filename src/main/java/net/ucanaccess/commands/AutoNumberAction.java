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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessConnection;


import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;

public class AutoNumberAction implements IFeedbackAction {
	private HashMap<String, Object> newAutoValues = new HashMap<String, Object>();
	private HashMap<String, Object> oldAutoValues = new HashMap<String, Object>();
	private Table table;
	
	public AutoNumberAction(Table table, Object[] memento,Object[] byAccess) throws SQLException {
		super();
		this.table = table;
		int i = 0;
		PreparedStatement ps = null;
		for (Column cl : table.getColumns()) {
			if (cl.isAutoNumber()) {
				UcanaccessConnection conn = UcanaccessConnection
						.getCtxConnection();
				Connection connHsqldb = conn.getHSQLDBConnection();
				String cn = SQLConverter.escapeIdentifier(cl.getName(),connHsqldb);
				Object cnOld = memento[i];
				Object cnNew = byAccess[i];
				if(cnNew instanceof String)cnNew=((String)cnNew).toUpperCase();
				oldAutoValues.put(cl.getName(), cnOld);
				newAutoValues.put(cl.getName(), cnNew);
				try {
					conn.setFeedbackState(true);
					String stmt = "UPDATE "
							+ SQLConverter.escapeIdentifier(table.getName(),connHsqldb)
							+ " SET " + cn + "=? WHERE " + cn + "=?";
					ps = connHsqldb.prepareStatement(stmt);
					ps.setObject(1, cnNew);
					ps.setObject(2, cnOld);
					ps.executeUpdate();
				if(cnNew instanceof Integer)
					conn.setGeneratedKey((Integer)cnNew);
					conn
							.setFeedbackState(false);
				} finally {
					if (ps != null)
						ps.close();
				}
			}
			++i;
		}
	}
	
	public void doAction(ICommand toChange) throws SQLException {
		if (!this.table.getName().equalsIgnoreCase(toChange.getTableName()))
			return;
		switch (toChange.getType()) {
		case DELETE:
		case UPDATE:
			AbstractCursorCommand acm = (AbstractCursorCommand) toChange;
			Map<String, Object> old = acm.getRowPattern();
			for (Map.Entry<String, Object> entry : oldAutoValues.entrySet()) {
				if (old.containsKey(entry.getKey())
						&& old.get(entry.getKey()).equals(entry.getValue())) {
					old.put(entry.getKey(), newAutoValues.get(entry.getKey()));
				}
			}
			break;
		case COMPOSITE:
			CompositeCommand cc = (CompositeCommand) toChange;
			for (ICommand ic : cc.getComposite()) {
				doAction(ic);
			}
		}
	}
}
