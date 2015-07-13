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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class BatchTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public BatchTest() {
		super();
	}
	
	public BatchTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE Tb (id LONG,name TEXT, age LONG) ");
		 
		Statement	st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO Tb VALUES(1,'Sophia', 33)");

			st.close();
		
	}
	
	public void testBatch() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.addBatch("UPDATE Tb SET [name]='ccc'");
			st.addBatch("UPDATE Tb SET age=95");
			st.executeBatch();
			checkQuery("select * from tb");
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testBatchPS() throws SQLException, IOException {
		PreparedStatement st = null;
		try {
			st = super.ucanaccess.prepareStatement("UPDATE Tb SET [name]=?,age=? ");
			
			st.setString(1, "ciao");
			st.setInt(2, 23);
			st.addBatch();
			st.setString(1, "ciao1");
			st.setInt(2, 43);
			st.addBatch();
			st.executeBatch();
			checkQuery("select * from tb");
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
