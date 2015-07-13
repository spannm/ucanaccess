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

import java.util.Date;

import org.hsqldb.types.JavaObjectData;

import net.ucanaccess.complex.Version;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;

import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

public class TriggerAppendOnly extends TriggerBase {
	public static int autorandom = -1;

	public void fire(int type, String name, String tableName, Object[] oldR,
			Object[] newR) {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		if (conn.isFeedbackState())
			return;
		try {
			Table t = this.getTable(tableName,conn);
			if (t == null)
				throw new RuntimeException(
						Logger.getMessage("TABLE_DOESNT_EXIST") + " :"
								+ tableName);
			int i = 0;
			for (Column cl : t.getColumns()) {
				if (cl.isAppendOnly()) {
					ColumnImpl verCol = (ColumnImpl)cl.getVersionHistoryColumn();
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
							
							Version[] newV =new Version[oldV.length + 1];
							for(int j=0;j<oldV.length;j++){
								newV[j+1]=oldV[j];
							}
							newV[0] = new Version(val, upTime);
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
