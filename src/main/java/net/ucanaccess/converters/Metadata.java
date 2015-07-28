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
package net.ucanaccess.converters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Metadata {
	
	private Connection conn;
	private final static String SCHEMA="CREATE SCHEMA UCA_METADATA AUTHORIZATION DBA";
	
	private final static String TABLES="CREATE  TABLE UCA_METADATA.TABLES(TABLE_ID INTEGER IDENTITY, TABLE_NAME LONGVARCHAR,ESCAPED_TABLE_NAME LONGVARCHAR, TYPE VARCHAR(5),UNIQUE(TABLE_NAME)) ";
	private final static String COLUMNS="CREATE MEMORY TABLE    " +
			"				UCA_METADATA.COLUMNS(COLUMN_ID INTEGER IDENTITY, COLUMN_NAME LONGVARCHAR,ESCAPED_COLUMN_NAME LONGVARCHAR, " +
			"				ORIGINAL_TYPE VARCHAR(20),COLUMN_DEF  LONGVARCHAR,IS_GENERATEDCOLUMN VARCHAR(3),TABLE_ID INTEGER, UNIQUE(TABLE_ID,COLUMN_NAME) )";
	
	private final static String PROP="CREATE MEMORY TABLE   UCA_METADATA.PROP(NAME LONGVARCHAR PRIMARY KEY, MAX_LEN INTEGER,DEFAULT_VALUE VARCHAR(20),DESCRIPTION LONGVARCHAR) ";
	
	public final static String SYSTEM_SUBQUERY="SYSTEM_SUBQUERY";
	private final static Object[][] PROP_DATA=new Object[][]{
		{"newdatabaseversion",8,null,"see ucanaccess website"},
		{"jackcessopener",500,null,"see ucanaccess web site"},
		{"password",500,null,"see ucanaccess web site"},
		{"memory",10, "true" ,"see ucanaccess web site"},
		{"lobscale",2,"2","see ucanaccess web site"},
		{"keepmirror",500,"2","see ucanaccess web site"},
		{"showschema",10,"false","see ucanaccess web site"},
		{"inactivitytimeout",10,"2","see ucanaccess web site"},
		{"singleconnection",10,"false","see ucanaccess web site"},
		{"lockmdb",10,"false","see ucanaccess web site"},
		{"openexclusive",500,"false","see ucanaccess web site"},
		{"remap",500,null,"see ucanaccess web site"},
		{"columnorder",10,"data","see ucanaccess web site"},
		{"mirrorfolder",500,null,"see ucanaccess web site"},
		{"ignorecase",10,"true","see ucanaccess web site"},
		{"sysschema",10,"false","see ucanaccess web site"},
		{"skipindexes",10,"false","see ucanaccess web site"},
		{"preventreloading",10,"false","see ucanaccess web site"},
		{"concatnulls",10,"false","see ucanaccess web site"}
		
	};
	
	private final static String COLUMNS_VIEW="CREATE VIEW   UCA_METADATA.COLUMNS_VIEW as " +
			"SELECT t.TABLE_NAME, c.COLUMN_NAME,t.ESCAPED_TABLE_NAME, c.ESCAPED_COLUMN_NAME,c.COLUMN_DEF,c.IS_GENERATEDCOLUMN," +
			"CASE WHEN(c.ORIGINAL_TYPE 	IN ('COUNTER' ,'GUID')) THEN 'YES' ELSE 'NO' END as IS_AUTOINCREMENT,c.ORIGINAL_TYPE " +
			"FROM UCA_METADATA.COLUMNS c INNER JOIN UCA_METADATA.TABLES t ON (t.TABLE_ID=c.TABLE_ID)";
	
	private final static String FK="ALTER TABLE UCA_METADATA.COLUMNS   " +
			"ADD CONSTRAINT UCA_METADATA_FK FOREIGN KEY (TABLE_ID) REFERENCES UCA_METADATA.TABLES (TABLE_ID) ON DELETE CASCADE";
	
	
	
	private final static String TABLE_RECORD="INSERT INTO UCA_METADATA.TABLES( TABLE_NAME,ESCAPED_TABLE_NAME, TYPE) VALUES(?,?,?)";
	private final static String COLUMN_RECORD="INSERT INTO UCA_METADATA.COLUMNS(COLUMN_NAME,ESCAPED_COLUMN_NAME,ORIGINAL_TYPE, IS_GENERATEDCOLUMN,TABLE_ID) " +
			"VALUES(?,?,?,'NO',?)";
	
	private final static String SELECT_COLUMN="SELECT DISTINCT c.COLUMN_NAME,c.ORIGINAL_TYPE IN('COUNTER','GUID') as IS_AUTOINCREMENT, c.ORIGINAL_TYPE='MONEY' as IS_CURRENCY  " +
			"				FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES  t " +
			"				ON(t.TABLE_ID=c.TABLE_ID ) WHERE t.ESCAPED_TABLE_NAME=nvl(?,t.ESCAPED_TABLE_NAME) AND c.ESCAPED_COLUMN_NAME=? ";
	
	
	private final static String SELECT_COLUMN_ESCAPED="SELECT c.ESCAPED_COLUMN_NAME" +
	"				FROM UCA_METADATA.COLUMNS  c INNER JOIN UCA_METADATA.TABLES  t " +
	"				ON(t.TABLE_ID=c.TABLE_ID ) WHERE t.TABLE_NAME=nvl(?,t.TABLE_NAME) AND c.COLUMN_NAME=? ";
	
	private final static String SELECT_TABLE_METADATA="SELECT TABLE_ID, TABLE_NAME FROM UCA_METADATA.TABLES WHERE ESCAPED_TABLE_NAME=? ";
	private final static String DROP_TABLE="DELETE FROM UCA_METADATA.TABLES WHERE TABLE_NAME=?";
	private final static String UPDATE_COLUMN_DEF="UPDATE UCA_METADATA.COLUMNS c SET c.COLUMN_DEF=? WHERE COLUMN_NAME=? " +
			" AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID) ";
	private static final String UPDATE_IS_GENERATEDCOLUMN = "UPDATE UCA_METADATA.COLUMNS c SET c.IS_GENERATEDCOLUMN='YES' WHERE COLUMN_NAME=? " +
			" AND EXISTS(SELECT * FROM UCA_METADATA.TABLES t WHERE t.TABLE_NAME=? AND t.TABLE_ID=c.TABLE_ID) ";
	
	
	
	public static enum Types{VIEW,TABLE}
	
	public Metadata(Connection conn) throws SQLException {
		super();
		this.conn = conn;
		
	}
	
	public void createMetadata() throws SQLException{
		Statement st=null;
		try{
			st=conn.createStatement();
			st.execute(SCHEMA);
			st.execute(PROP);
			st.execute(TABLES);
			st.execute(COLUMNS);
			st.execute(FK);
			st.execute(COLUMNS_VIEW);
			loadProp();
		}finally{
			if(st!=null)st.close();
		}
	}
	
	public void loadProp() throws SQLException{
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement("INSERT INTO UCA_METADATA.PROP( NAME , MAX_LEN , DEFAULT_VALUE , DESCRIPTION) VALUES(?,?,?,?)");
			for(Object[] ob:PROP_DATA){
				ps.setObject(1, ob[0]);
				ps.setObject(2, ob[1]);
				ps.setObject(3, ob[2]);
				ps.setObject(4, ob[3]);
				ps.execute();
			}
		
		}
		
		finally{
			if(ps!=null)ps.close();
		}
		
	}
		
		
		
	public Integer newTable(String name,String escaped,Types type) throws SQLException{
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(TABLE_RECORD,PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setString(1, name);
			ps.setString(2, escaped);
			ps.setString(3, type.name());
			ps.executeUpdate();
			ResultSet rs= ps.getGeneratedKeys();
			rs.next();
			
			return rs.getInt(1);  
		}
		catch(SQLException e)   {
			return getTableId(escaped);
		}
		finally{
			if(ps!=null)ps.close();
		}
	}
	
	public void newColumn(String name,String escaped,String originalType, Integer idTable) throws SQLException{
		if(idTable<0)return;
		PreparedStatement ps=null;
		try{ 
			ps=conn.prepareStatement(COLUMN_RECORD);
			ps.setString(1, name);
			ps.setString(2, escaped);
			ps.setString(3, originalType);
			ps.setInt(4, idTable);
			ps.executeUpdate();
		}catch(SQLException e)   {
			
		}
		
		finally{
			if(ps!=null)ps.close();
		}
	}
	
	
	public String getColumnName(String tableName,String columnName) throws SQLException {
		PreparedStatement ps=null;
		try{
			boolean camb=SYSTEM_SUBQUERY.equals(tableName);
			tableName= camb?null:tableName;
			ps=conn.prepareStatement(SELECT_COLUMN);
			ps.setString(1, tableName);
			ps.setString(2, columnName);
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				String res=rs.getString("COLUMN_NAME");
				if(!camb||!rs.next())
				return res;
				
			}
			 return null;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	
	public String getEscapedColumnName(String tableName,String columnName) throws SQLException {
		PreparedStatement ps=null;
		try{
			tableName= SYSTEM_SUBQUERY.equals(tableName)?null:tableName;
			ps=conn.prepareStatement(SELECT_COLUMN_ESCAPED);
			ps.setString(1, tableName);
			ps.setString(2, columnName);
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				return rs.getString("ESCAPED_COLUMN_NAME");
			}
			else return null;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	public boolean isAutoIncrement(String tableName,String columnName) throws SQLException {
		PreparedStatement ps=null;
		tableName= SYSTEM_SUBQUERY.equals(tableName)?null:tableName;
		try{
			ps=conn.prepareStatement(SELECT_COLUMN);
			ps.setString(1, tableName);
			ps.setString(2, columnName);
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				return rs.getBoolean("IS_AUTOINCREMENT");
			}
			else return false;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	
	public boolean isCurrency(String tableName,String columnName) throws SQLException {
		PreparedStatement ps=null;
		tableName= SYSTEM_SUBQUERY.equals(tableName)?null:tableName;
		try{
			ps=conn.prepareStatement(SELECT_COLUMN);
			ps.setString(1, tableName);
			ps.setString(2, columnName);
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				return rs.getBoolean("IS_CURRENCY");
			}
			else return false;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	public Integer getTableId(String escapedName) throws SQLException {
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(SELECT_TABLE_METADATA);
			ps.setString(1, escapedName);
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				return rs.getInt("TABLE_ID");
			}
			else return -1;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	
	public String getTableName(String escapedName) throws SQLException {
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(SELECT_TABLE_METADATA);
			ps.setString(1, escapedName);
			
			ResultSet rs= ps.executeQuery();
			if(rs.next()){
				return rs.getString("TABLE_NAME");
			}
			else return null;
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	public void dropTable(String tableName) throws SQLException {
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(DROP_TABLE);
			ps.setString(1, tableName);
			ps.execute();
			
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	public void columnDef(String tableName,String columnName,String def) throws SQLException {
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(UPDATE_COLUMN_DEF);
			ps.setString(1, def);
			ps.setString(2, columnName);
			ps.setString(3, tableName);
			ps.execute();
			
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
	
	public void calculatedField(String tableName,String columnName) throws SQLException {
		PreparedStatement ps=null;
		try{
			ps=conn.prepareStatement(UPDATE_IS_GENERATEDCOLUMN);
			ps.setString(1, columnName);
			ps.setString(2, tableName);
			ps.execute();
			
			
		}finally{
			if(ps!=null)ps.close();
		}
	}
}
