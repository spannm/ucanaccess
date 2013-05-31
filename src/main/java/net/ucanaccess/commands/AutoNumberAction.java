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
				String cn = SQLConverter.escapeIdentifier(cl.getName());
				Object cnOld = memento[i];
				Object cnNew = byAccess[i];
				oldAutoValues.put(cl.getName(), cnOld);
				newAutoValues.put(cl.getName(), cnNew);
				try {
					conn.setFeedbackState(true);
					String stmt = "UPDATE "
							+ SQLConverter.escapeIdentifier(table.getName())
							+ " SET " + cn + "=? WHERE " + cn + "=?";
					ps = connHsqldb.prepareStatement(stmt);
					ps.setObject(1, cnNew);
					ps.setObject(2, cnOld);
					ps.executeUpdate();
					UcanaccessConnection.getCtxConnection()
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
