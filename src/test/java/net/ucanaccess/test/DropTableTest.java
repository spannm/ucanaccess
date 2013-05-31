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
	}
	
	public void createSimple(String a, Object[][] ver) throws SQLException,
			IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO AAAn VALUES ('33A',11,'" + a + "'   )");
			st.execute("INSERT INTO AAAn VALUES ('33B',111,'" + a + "'    )");
			checkQuery("select * from AAAn", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testDrop() throws SQLException, IOException {
		Statement st = null;
		try {
			createSimple("a", new Object[][] { { "33A", 11, "a" },
					{ "33B", 111, "a" } });
			st = super.ucanaccess.createStatement();
			st.executeUpdate("DROP TABLE AAAn");
			st
					.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
			createSimple("b", new Object[][] { { "33A", 11, "b" },
					{ "33B", 111, "b" } });
			dump("select * from AAAn");
		} finally {
			if (st != null)
				st.close();
		}
	}
}
