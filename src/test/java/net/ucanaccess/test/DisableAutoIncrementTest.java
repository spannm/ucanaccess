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
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class DisableAutoIncrementTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public DisableAutoIncrementTest() {
		super();
	}
	
	public DisableAutoIncrementTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE CT (id COUNTER PRIMARY KEY ,descr TEXT) ");
		 
	}
	
	public void testGuid() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("CREATE TABLE CT1 (id guid PRIMARY KEY ,descr TEXT) ");
			st.execute("INSERT INTO CT1 (descr) VALUES ('CIAO')");
			
			checkQuery("select * from CT1");
			
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testDisable() throws SQLException, IOException {
		Statement st = null;
		try {
			boolean exc=false;
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
			st.execute("DISABLE AUTOINCREMENT ON CT ");
			try{
				st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
			}catch(Exception e){
				exc=true;
			}
			assertTrue(exc);
			st.execute("enable AUTOINCREMENT ON CT ");
			st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
