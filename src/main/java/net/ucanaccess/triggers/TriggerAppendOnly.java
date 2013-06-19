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

import java.util.Arrays;
import java.util.Date;

import org.hsqldb.types.JavaObjectData;

import net.ucanaccess.complex.Version;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;

import com.healthmarketscience.jackcess.Table;

public class TriggerAppendOnly extends TriggerBase {
	public static int autorandom = -1;

	public void fire(int type, String name, String tableName, Object[] oldR,
			Object[] newR) {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		if (conn.isFeedbackState())
			return;
		try {
			Table t = conn.getDbIO().getTable(tableName);
			if (t == null)
				throw new RuntimeException(
						Logger.getMessage("TABLE_DOESNT_EXIST") + " :"
								+ tableName);
			int i = 0;
			for (Column cl : t.getColumns()) {
				if (cl.isAppendOnly()) {
					Column verCol = cl.getVersionHistoryColumn();
					Date upTime = new Date();
					String val = newR[i] == null ? null : newR[i].toString();
					if (type == TriggerBase.INSERT_BEFORE_ROW)
						newR[verCol.getColumnNumber()] = new JavaObjectData(
								new Version[] { new Version(val, upTime) });
					else if (type == TriggerBase.UPDATE_BEFORE_ROW
							&& (oldR[i] != null || newR[i] != null)) {
						if ((oldR[i] == null && newR[i] != null)
								|| (oldR[i] != null && newR[i] == null)
								|| (!oldR[i].equals(newR[i]))) {
							Version[] oldV = (Version[]) ((JavaObjectData)oldR[verCol
									.getColumnNumber()]).getObject();
							Version[] newV = Arrays.copyOf(oldV,
									oldV.length + 1);
							newV[oldV.length] = new Version(val, upTime);
							newR[verCol.getColumnNumber()] = new JavaObjectData(
									newV);
						}
					}
				}
				++i;
			}
		} catch (Exception e) {
			throw new TriggerException(e.getMessage());
		}
	}
}
