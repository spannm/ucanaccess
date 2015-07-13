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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class GeneratedKeysTest extends UcanaccessTestBase {
	private String tableName = "T_Key";
	public GeneratedKeysTest() {
		super();
	}
	
	public GeneratedKeysTest(FileFormat accVer) {
		super(accVer);
	}
	protected void setUp() throws Exception {
		super.setUp();
			executeCreateTable("CREATE TABLE "
					+ tableName
					+ " ( Z COUNTER PRIMARY KEY, B char(4) )");
	}
	
	public void testGeneratedKeys() throws SQLException, IOException {
	
			PreparedStatement ps=  super.ucanaccess.prepareStatement("INSERT INTO " + tableName
					+ " (B) VALUES (?)");
			ps.setString(1,"");
			ps.execute();
			ResultSet rs=ps.getGeneratedKeys();
			rs.next();
			assertEquals(1, rs.getInt(1));
			ps.close();
			ps=super.ucanaccess.prepareStatement("Select @@identity ");
			rs=ps.executeQuery();
			rs.next();
			assertEquals(1, rs.getInt(1));
			Statement st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO " + tableName
					+ " (B) VALUES ('W')");
			
			checkQuery("Select @@identity ",2);
			 rs=st.getGeneratedKeys();
			rs.next();
			assertEquals(2, rs.getInt(1));
			
			
		
	}
}
