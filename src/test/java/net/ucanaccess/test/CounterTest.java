/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

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
