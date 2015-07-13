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

public class CounterTest extends UcanaccessTestBase {
	private String tableName = "T_BBB";
	public CounterTest() {
		super();
	}
	
	public CounterTest(FileFormat accVer) {
		super(accVer);
	}
	protected void setUp() throws Exception {
		super.setUp();
			executeCreateTable("CREATE TABLE "
					+ tableName
					+ " ( Z COUNTER PRIMARY KEY, B char(4), C blob, d TEXT )");
	}
	
	public void testCreateTypes() throws SQLException, IOException {
	
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			
			st.execute("INSERT INTO " + tableName
					+ " (B,C,D) VALUES ('W' ,NULL,NULL)");
			st.execute("INSERT INTO " + tableName
					+ " (B,C,D) VALUES ('B',NULL,NULL  )");
			Object[][] ver = { { 1, "W   ", null, null },
					{ 2, "B   ", null, null } };
			checkQuery("select * from " + tableName + " order by Z", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
}
