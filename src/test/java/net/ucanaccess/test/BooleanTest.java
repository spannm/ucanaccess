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
import java.text.ParseException;

import java.util.Locale;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class BooleanTest extends UcanaccessTestBase {
	private static boolean init;

	public BooleanTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public BooleanTest(FileFormat accVer) {
		super(accVer);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		if (!init) {
			Statement st = null;
		
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE tblMain (ID int NOT NULL PRIMARY KEY,company TEXT NOT NULL,  Closed YESNO); ");
			st.close();
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("insert into tblMain (id,company)values(1,'pippo')");
			st
			.executeUpdate("update tblMain set closed=yes");
			
			st
			.executeUpdate("insert into t (pk)values('pippo')");
			init = true;
		}

	}
	
	
	
	
	public void testCreate() throws SQLException, IOException, ParseException {
		dump("select * from  tblMain");
		dump("select * from  t");
		checkQuery("select * from  tblMain");
		checkQuery("select * from  t");
	}
	
	public String getAccessPath() {
		return  "net/ucanaccess/test/resources/bool.accdb";
	}
	

	
	
}
