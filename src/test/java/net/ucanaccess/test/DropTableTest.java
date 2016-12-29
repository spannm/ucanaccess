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

public class DropTableTest extends UcanaccessTestBase {
	
	public DropTableTest() {
		super();
	}
	
	public DropTableTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
		executeCreateTable("CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ",1);
	}
	
	public void createSimple(String tn,String a, Object[][] ver) throws SQLException,
			IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO "+tn+" VALUES ('33A',11,'" + a + "'   )");
			st.execute("INSERT INTO "+tn+" VALUES ('33B',111,'" + a + "'    )");
			checkQuery("select * from "+tn+ " ORDER BY c", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
	

	
	public void testDrop() throws SQLException, IOException {
		Statement st = null;
		try {
			//super.ucanaccess.setAutoCommit(false);
			createSimple("AAAn","a", new Object[][] { { "33A", 11, "a" },
					{ "33B", 111, "a" } });
			st = super.ucanaccess.createStatement();
			st.executeUpdate("DROP TABLE AAAn");
			//super.ucanaccess.commit();
			st
					.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
			createSimple("AAAn","b", new Object[][] { { "33A", 11, "b" },
					{ "33B", 111, "b" } });
			dump("select * from AAAn");
			super.ucanaccess.commit();
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
	public void testDropBlank() throws SQLException, IOException {
		Statement st = null;
		try {
			//super.ucanaccess.setAutoCommit(false);
			createSimple("[AAA n]","a", new Object[][] { { "33A", 11, "a" },
					{ "33B", 111, "a" } });
			st = super.ucanaccess.createStatement();
			st.executeUpdate("DROP TABLE [AAA n]");
			//super.ucanaccess.commit();
			st
					.execute("CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
			createSimple("[AAA n]","b", new Object[][] { { "33A", 11, "b" },
					{ "33B", 111, "b" } });
			dump("select * from [AAA n]");
			super.ucanaccess.commit();
		} finally {
			if (st != null)
				st.close();
		}
	}
}
