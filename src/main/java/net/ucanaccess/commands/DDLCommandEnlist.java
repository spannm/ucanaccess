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






import java.util.HashMap;

import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.converters.SQLConverter.DDLType;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

import com.healthmarketscience.jackcess.Database;

public class DDLCommandEnlist {
	private String[] types;
	private String[] defaults;
	private Boolean[] notNulls;
	private  HashMap<String,String> columnMap= new HashMap<String,String>();
	
	
	private void enlistCreateTable(String sql, DDLType ddlType)
			throws SQLException {
		String tn = ddlType.getDBObjectName(sql);
		UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
		String execId = UcanaccessConnection.getCtxExcId();
		Connection hsqlConn = ac.getHSQLDBConnection();
		Database db = ac.getDbIO();
		LoadJet lfa = new LoadJet(hsqlConn, db);
		String ntn=tn;
		if(tn.startsWith("[")&&tn.endsWith("]")){
			ntn=SQLConverter.escapeIdentifier(tn.substring(1, tn.length()-1));
		}
		
		lfa.synchronisationTriggers(ntn, true,true);
		CreateTableCommand c4io;
       if(ddlType.equals(DDLType.CREATE_TABLE)){
    	   parseTypesFromCreateStatement(sql);
    	   c4io=new CreateTableCommand(
				tn, execId,this.columnMap, this.types,this.defaults,this.notNulls);
    	   }
       else  {
    	   c4io=new CreateTableCommand(tn, execId);
       }
      
		ac.add(c4io); 
		if(!ac.getAutoCommit()){
			ac.commit();
		}
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
	private void parseTypesFromCreateStatement(String sql) throws SQLException {
	
		int startDecl = sql.indexOf('(');
		int endDecl = sql.lastIndexOf(')');
		
		if (startDecl >= endDecl) {
			throw new UcanaccessSQLException(ExceptionMessages.INVALID_CREATE_STATEMENT);
		}
		String decl = sql.substring(startDecl + 1, endDecl);
		String[] tokens = decl.split(",");
		
		ArrayList<String> typeList = new ArrayList<String>() ;
		ArrayList<String> defaultList = new ArrayList<String>() ;
		this.columnMap = new HashMap<String,String>() ;
		ArrayList<Boolean> notNullList = new ArrayList<Boolean>() ;
		for (int j = 0; j < tokens.length; ++j) {
			String tknt=tokens[j].trim();
			
			String[] colDecls = tknt.split("[\\s\n\r]+");
			
			if(colDecls[0].startsWith("[")&&tknt.substring(1).indexOf("]")>0){
				for(int k=0;k<colDecls.length;k++){
					if(colDecls[k].endsWith("]")){
						String[] colDecls0=new String[colDecls.length-k];
						colDecls0[0]=tknt.substring(1,tknt.indexOf("]"));
						for(int y=1;y<colDecls0.length;y++){
							colDecls0[y]=colDecls[y+k];
						}
						colDecls=colDecls0;
						break;
					}
				}
			}
			String escaped=(SQLConverter.isListedAsKeyword(colDecls[0].toUpperCase()))?
					colDecls[0].toUpperCase():SQLConverter.basicEscapingIdentifier(colDecls[0]);
			columnMap.put(escaped,colDecls[0]);
		 
			
			boolean reset=false;
			if(tknt.matches("[\\s\n\r]*\\d+[\\s\n\r]*\\).*")){
				reset=true;
				tknt=tknt.substring(tknt.indexOf(")")+1).trim();
				colDecls = tknt.split("[\\s\n\r]+");
			}
			
			if (!reset&&colDecls.length< 2) {
				continue;
			}
			boolean decDef=false;
			if(!reset){
				if(colDecls[1]!=null&&colDecls[1].toUpperCase().startsWith("NUMERIC(")){
					colDecls[1]="NUMERIC";
					decDef=true;
				}
				typeList.add(colDecls[1]);
				
			}
			
			if(colDecls.length>2
					&&"not".equalsIgnoreCase(colDecls[colDecls.length-2])
					&&"null".equalsIgnoreCase(colDecls[colDecls.length-1])
					){
				notNullList.add(true);
			}else if(!decDef){
				notNullList.add(false); 
			}
			
			if(!decDef){
				defaultList.add(value(SQLConverter.getDDLDefault(tknt)));
			}
	
		this.types= (String[])typeList.toArray(new String[typeList.size()]);
		
		this.defaults=(String[])defaultList.toArray(new String[defaultList.size()]);
		this.notNulls=(Boolean[])notNullList.toArray(new Boolean[notNullList.size()]);
		
	}
	}

	private String value(String value) {
		if(value==null)return null;
		if(value.startsWith("\"")&&value.endsWith("\"")){
			return  value.substring(1, value.length()-1).replaceAll("\"\"", "\"");
		}
		if(value.startsWith("'")&&value.endsWith("'")){
			return (value.substring(1, value.length()-1).replaceAll("''", "'"));
		}
		return value;
	}
	
	
}
