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

import java.sql.SQLException;


import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

public class TriggerAutoNumber extends TriggerBase {
	 public static int autorandom=-1;
	private static final String GUID_PATTERN = "\\s*[{]?([\\p{XDigit}]{8})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{12})[}]?\\s*";
	
	public void fire(int type, String name, String tableName, Object[] oldR,
			Object[] newR) {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		if (conn.isFeedbackState())
			return;
		try {
			Table t = getTable(tableName,conn);
			if(t==null)throw new RuntimeException(Logger.getMessage("TABLE_DOESNT_EXIST")+" :"+tableName);
			int i = 0;
			for (Column cli : t.getColumns()) {
				ColumnImpl cl=(ColumnImpl)cli;
				if (cl.isAutoNumber()) {
					if (type == TriggerAutoNumber.INSERT_BEFORE_ROW) {
						if (cl.getAutoNumberGenerator().getType().equals(
								DataType.GUID))
							newR[i] = cl.getAutoNumberGenerator().getNext(
									cl.getAutoNumberGenerator().getLast());
						else if (cl.getAutoNumberGenerator().getType().equals(
								DataType.LONG)) {
							
							newR[i] = 	AutoNumberManager.getNext(cl);
							
						}
					} else if(type == TriggerAutoNumber.UPDATE_BEFORE_ROW&&cl.getAutoNumberGenerator().getType().equals(
							DataType.LONG)){
						if(!oldR[i].equals(newR[i])){
							throw new RuntimeException("Cannot update autoincrement column");
						}
					}
					
					
					else if (cl.getAutoNumberGenerator().getType().equals(
							DataType.GUID)) {
						validateGUID(newR[i]);
					}
				}else if(DataType.BOOLEAN.equals(cl.getType())){
					if(newR[i]==null){
						newR[i]=false;
					}
				}
				++i;
			}
		} catch (Exception e) {
			throw new TriggerException(e.getMessage());
		}
	}
	
	private void validateGUID(Object guid) throws SQLException {
		if (guid != null && guid instanceof String) {
			String guidS = (String) guid;
			if (guidS.length() != 38 || !guidS.matches(GUID_PATTERN)) {
				throw new SQLException("Invalid guid format "+guidS);
			}
		}
	}
}
