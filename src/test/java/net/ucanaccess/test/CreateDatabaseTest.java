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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class CreateDatabaseTest extends UcanaccessTestBase {
	public CreateDatabaseTest() {
		super();
	}
	
	public CreateDatabaseTest(FileFormat accVer) {
		super(accVer);
	}
	
	
	protected void setUp() throws Exception {}
	
	
	public void testNewDatabase() throws Exception {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		File fileMdb = new File("./"+Math.random()+".mdb");
		Connection ucanaccessConnection = null;
		try {
			String url =UcanaccessDriver.URL_PREFIX
			+ fileMdb.getAbsolutePath()+";newdatabaseversion="+FileFormat.V2003.name();
			
			ucanaccessConnection = DriverManager.getConnection(url, "", "");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNotNull(ucanaccessConnection);
		System.out.println("Mdb was successfull created @ "+fileMdb .getAbsolutePath());
		Statement st = null;
		st =ucanaccessConnection.createStatement();
		st.execute(" CREATE TABLE AAA ( baaaa text(3) PRIMARY KEY,A long default 3, C text(4) ) ");
		st.close();
		st =ucanaccessConnection.createStatement();
		st.execute("INSERT INTO AAA(baaaa,c) VALUES ('33A','G'   )");
		st.execute("INSERT INTO AAA VALUES ('33B',111,'G'   )");
		super.ucanaccess=ucanaccessConnection;
		dump("SELECT * FROM AAA");
		
	
	}
}
