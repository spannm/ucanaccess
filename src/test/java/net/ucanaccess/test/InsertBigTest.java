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

public class InsertBigTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public InsertBigTest() {
		super();
	}
	
	public InsertBigTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE Tbig (id LONG,descr memo) ");
		
	}
	
	public void testBig() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			int id = 6666554;
			String s="t";
			for(int i=0;i<10000;i++){
				s+="t\n";
			}
			st.execute("INSERT INTO Tbig (id,descr)  VALUES( " + id
					+ ",'"+s+"')");
			dump("select * from Tbig");
			
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
