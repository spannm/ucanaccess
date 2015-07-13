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

import com.healthmarketscience.jackcess.Database.FileFormat;

public class PivotTest extends UcanaccessTestBase {
	
	public PivotTest() {
		super();
	}
	
	public PivotTest(FileFormat accVer) {
		super(accVer);
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
	}
	
	@Override
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/pivot.mdb";
	}
	
	
	public void testPivot() throws SQLException, IOException, ParseException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			dump("select * from Table1_trim");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE',1234.56,#2003-12-03#   )");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE MIO',134.46,#2003-12-03#   )");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('STA IN FRUNTE A MEEE',1344.46,#2003-12-05#   )");
			super.initVerifyConnection();
			dump("select * from Table1_trim");
			checkQuery("select * from Table1_trim");
		} finally {
			if (st != null)
				st.close();
		}
	}
}
