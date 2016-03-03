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


import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.SQLException;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class ParametersTest extends UcanaccessTestBase {
	public ParametersTest() {
		super();
	}
	
	public ParametersTest(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/Parameters.accdb";
	}
	
	
	
	public void testParameters() throws SQLException, IOException{
		
		dump("select * from tq");
		dump("select * from z");
		dump("select * from [queryWithParameters]");
		dump("select * from table(queryWithParameters(#1971-03-13#,'hi babe'))");
		checkQuery("select count(*) from [ab\"\"\"xxx]",3);
		CallableStatement cs= ucanaccess.prepareCall("{call Insert_from_select_abxxx(?,?,?)}");
		cs.setString(1, "2");
		cs.setString(2,"YeaH!!!!");
		cs.setString(3,"u can see it works");
		cs.executeUpdate();
		
		
		dump("select * from [ab\"\"\"xxx]");
		checkQuery("select count(*) from [ab\"\"\"xxx]",6);
		//metaData();
		cs= ucanaccess.prepareCall("{call InsertWithFewParameters(?,?,?)}");
		
		cs.setString(1, "555");
		cs.setString(2,"YeaH!ddd!!!");
		cs.setDate(3,new java.sql.Date(System.currentTimeMillis()));
		cs.executeUpdate();
		cs.executeUpdate();
		cs.executeUpdate();
		dump("select * from [table 1]");
	
	}
	
	private void metaData() throws SQLException, IOException{
		DatabaseMetaData dbmd=ucanaccess.getMetaData();
		System.out.println(dbmd.getSystemFunctions());
		System.out.println(dbmd.getStringFunctions());
		dump(dbmd.getFunctions("PUBLIC", "PUBLIC", null));
		dump(dbmd.getProcedures("PUBLIC", "PUBLIC", null));
	}
	
}
