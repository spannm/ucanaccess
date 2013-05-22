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
import java.sql.SQLException;
import java.util.ArrayList;

import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.SQLConverter.DDLType;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;


import com.healthmarketscience.jackcess.Database;

public class DDLCommandEnlist {
	private void enlistCreateTable(String sql, DDLType ddlType)
			throws SQLException {
		String tn = ddlType.getDBObjectName(sql);
		UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
		String execId = UcanaccessConnection.getCtxExcId();
		Connection hsqlConn = ac.getHSQLDBConnection();
		Database db = ac.getDbIO();
		LoadJet lfa = new LoadJet(hsqlConn, db);
		lfa.synchronisationTriggers(tn, true);
		CreateTableCommand c4io = ddlType.equals(DDLType.CREATE_TABLE) ? new CreateTableCommand(
				tn, execId, parseTypesFromCreateStatement(sql))
				: new CreateTableCommand(tn, execId);
		ac.add(c4io);
	}
	
	public void enlistDDLCommand(String sql, DDLType ddlType)
			throws SQLException {
		switch (ddlType) {
		case CREATE_TABLE:
		case CREATE_TABLE_AS_SELECT:
			enlistCreateTable(sql, ddlType);
			break;
		case DROP_TABLE:
			enlistDropTable(sql, ddlType);
			break;
		}
	}
	
	private void enlistDropTable(String sql, DDLType ddlType) {
		String tn = ddlType.getDBObjectName(sql);
		String execId = UcanaccessConnection.getCtxExcId();
		UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
		DropTableCommand c4io = new DropTableCommand(tn, execId);
		ac.add(c4io);
	}
	//getting AUTOINCREMENT and GUID
	private String[] parseTypesFromCreateStatement(String sql) throws SQLException {
		int startDecl = sql.indexOf('(');
		int endDecl = sql.lastIndexOf(')');
		
		if (startDecl >= endDecl) {
			throw new UcanaccessSQLException(ExceptionMessages.INVALID_CREATE_STATEMENT);
		}
		String decl = sql.substring(startDecl + 1, endDecl);
		String[] tokens = decl.split(",");
		ArrayList<String> typeList = new ArrayList<String>() ;
		for (int j = 0; j < tokens.length; ++j) {
			String tknt=tokens[j].trim();
			if(tknt.matches("\\s*\\d+\\s*\\)")){
				
				continue;
			}
			String[] colDecls = tknt.split("\\s+");
		
			if (colDecls.length < 2) {
				throw new UcanaccessSQLException(ExceptionMessages.INVALID_CREATE_STATEMENT);
			}
			typeList.add(colDecls[1]);
		}
		return (String[])typeList.toArray(new String[typeList.size()]);
	}
}
