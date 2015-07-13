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

import java.util.Map;

import net.ucanaccess.commands.UpdateCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;


import com.healthmarketscience.jackcess.Table;

public class TriggerUpdate extends TriggerBase {
	public void fire(int type, String name, String tableName, Object[] oldR,
			Object[] newR) {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		if (conn.isFeedbackState())
			return;
		String execId = UcanaccessConnection.getCtxExcId();
		try {
			Table t = getTable(tableName,conn);
			super.convertRowTypes(oldR, t);
			super.convertRowTypes(newR, t);
			if (valuesChanged(oldR, newR)) {
				Map<String, Object> rowPattern = getRowPattern(oldR, t);
				UpdateCommand c4j = new UpdateCommand(t, rowPattern, newR,
						execId);
				conn.add(c4j);
			}
		} catch (Exception e) {
			throw new  TriggerException(e.getMessage());
		}
	}
	
	public boolean valuesChanged(Object[] oldR, Object[] newR) {
		if (oldR.length != newR.length) {
			return true;
		}
		for (int i = 0; i < oldR.length; ++i) {
			if (oldR[i] == null ^ newR[i] == null)
				return true;
			if (oldR[i] != null && newR[i] != null && !oldR[i].equals(newR[i])) {
				return true;
			}
		}
		return false;
	}
}
