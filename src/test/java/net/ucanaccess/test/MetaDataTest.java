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
package net.ucanaccess.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;


import com.healthmarketscience.jackcess.Database.FileFormat;

public class MetaDataTest extends UcanaccessTestBase {
	public MetaDataTest() {
		super();
	}

	public MetaDataTest(FileFormat accVer) {
		super(accVer);
	}

	public String getAccessPath() {
		return "net/ucanaccess/test/resources/badDB.accdb";
	}
	
	
	public void testCreateBadMetadata() throws Exception {
		Connection conn = ucanaccess;
		Statement st = conn.createStatement();
		st.execute("create table [健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] guiD PRIMARY KEY, [Sometime I wonder who I am ] text )");
		st.execute("insert into [健康] ([Sometime I wonder who I am ] ) values ('I''m a crazy man')");
		   checkQuery("select * from [健康] ");
	   System.out.println("crazy names in create table...");
		   dump("select * from [健康]");
		st.execute("create table [123456 nn%&/健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] aUtoIncrement PRIMARY KEY, [Sometime I wonder who I am ] text, [Πλήθος Αντιγράφων] CURRENCY,[ជំរាបសួរ] CURRENCY,[ЗДОРОВЫЙ] CURRENCY,[健康] CURRENCY,[健康な] CURRENCY,[किआओ ] CURRENCY default 12.88, [11q3 ¹²³¼½¾ß€] text(2), unique ([किआओ ] ,[健康な]) )");
		st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[健康な],[किआओ ] ) VALUES('I''m a wonderful forty',10.56,10.33,13,14)");
		PreparedStatement ps = super.ucanaccess.prepareStatement(
				"SELECT *  FROM [123456 nn%&/健康]", ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
		ResultSet rs = ps.executeQuery();
		 rs.moveToInsertRow();
		 
		  rs.updateString("Sometime I wonder who I am ", "Growing old without emotions");
		  rs.updateString("11q3 ¹²³¼½¾ß€", "康");
		  rs.insertRow();
		  System.out.println("crazy names in create table with updatable resultset...");
		  dump("select * from [123456 nn%&/健康]");
		  
		try{
		st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[किआओ ] ,[健康な]) VALUES('I''m a wonderful forty',11,11,14,13)");
		}catch(Exception e){
			System.out.println("ok,  unique constraint gotten");
		}
		st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[किआओ ] ,[健康な]) VALUES('I''m a wonderful forty',11,11,14.01,13)");
		try{
			st.execute("update [123456 nn%&/健康] set [健康な]=13,  [किआओ ]=14");
		}catch(Exception e){
			System.out.println("ok,  unique constraint gotten");
		}
		
		
		dump("select * from [123456 nn%&/健康]");
		
		
	  
	}

	public void testBadMetadata() throws Exception {
		dump("SELECT * FROM NOROMAN");
		Connection conn = ucanaccess;
		Statement st = conn.createStatement();
		ResultSetMetaData rsmd = st.executeQuery("SELECT * FROM NOROMAN").getMetaData();
		assertTrue(rsmd.isAutoIncrement(1));
		assertTrue(rsmd.isCurrency(6));
		DatabaseMetaData dbmd=	this.ucanaccess.getMetaData();
		
		ResultSet rs=dbmd.getTables(null, null, "NOROMAn", null);//noroman tableName
		System.out.println("Noroman characters...");
		dump(rs);
	   rs=dbmd.getColumns(null, null, "NOROMAn", null);//noroman tableName
		dump(rs);
		 rs=dbmd.getColumns(null, null, "%ROMAn", null);//noroman tableName
			dump(rs);
			System.out.println("getColumns...");
			 rs=dbmd.getColumns(null, null, "Πλήθ%", null);//noroman tableName
				dump(rs);
				rs=dbmd.getColumns(null, null, "%健康",null);
				dump(rs);
				System.out.println("getColumnPrivileges...");
			 rs=dbmd.getColumnPrivileges(null, null, "NOROMAn", null);//noroman tableName
				dump(rs);
				// rs=dbmd.getColumnPrivileges(null, null, null, null);
				System.out.println("getExportedKeys...");
				 rs=dbmd.getExportedKeys(null, null, "??###")	;
				 dump(rs);
				 System.out.println("getImportedKeys...");
				 rs=dbmd.getImportedKeys(null, null, "Tabella1")	;
				 dump(rs);
				 System.out.println("getPrimaryKeys...");
				 rs=dbmd.getPrimaryKeys(null, null, "Tabella1")	;
				 dump(rs);
				 System.out.println("getIndexInfo...");
				 rs=dbmd.getIndexInfo(null, null, "Tabella1",false,false)	;
				 dump(rs);
				 System.out.println("getCrossReference...");
				 rs=dbmd.getCrossReference(null, null,"??###",null,null,"Tabella1" )	;
				 dump(rs);
				 System.out.println("getVersionColumns...");
				 rs=dbmd.getVersionColumns(null, null,"Πλήθος" )	;
				 dump(rs);
				 System.out.println("getClientInfoProperties...");
				 rs=dbmd.getClientInfoProperties()	;
				 dump(rs);
				 System.out.println("getTablePrivileges...");
				 rs=dbmd.getTablePrivileges(null, null,"??###")	;
				 dump(rs);
				 System.out.println("getTables...");
				 rs=dbmd.getTables(null, null,"??###", new String[]{"TABLE"})	;
				 dump(rs);
				
				 rs=dbmd.getTables(null, null,null, new String[]{"VIEW"})	;
				 dump(rs);
				 System.out.println(".getBestRowIdentifier...");
				 rs=dbmd.getBestRowIdentifier(null, null, "??###", 0, true)	;
				 dump(rs);
				 rs=dbmd.getBestRowIdentifier(null, null, "??###", 33, true)	;
				 dump(rs);
				 System.out.println("getTypesInfo...");
				 rs=dbmd.getTypeInfo();
				 dump(rs);
	}
}
