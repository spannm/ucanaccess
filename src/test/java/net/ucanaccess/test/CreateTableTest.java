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

public class CreateTableTest extends UcanaccessTestBase {
	
	
	public CreateTableTest() {
		super();
	}
	
	public CreateTableTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable(" CREATE \nTABLE AAA ( baaaa text PRIMARY KEY,A long default 3, C text(255)) ");
			
	}
	

	
	public void testCreateSimple() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO AAA(baaaa,c) VALUES ('33A','G'   )");
			st.execute("INSERT INTO AAA VALUES ('33B',111,'G'   )");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select * from AAA order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testCreateAsSelect() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE AAA_BIS as (select * from AAA) 	WITH DATA");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select * from AAA_bis order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testCreateAsSelect2() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE AAA_TRIS as (select * from AAA) WITH no DATA ");
			st.execute("INSERT INTO AAA_TRIS SELECT * from AAA_bis");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select * from AAA_tris order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}
}
