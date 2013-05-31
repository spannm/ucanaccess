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

public class AccessLikeTest extends UcanaccessTestBase {
	public AccessLikeTest() {
		super();
	}
	
	public AccessLikeTest(FileFormat accVer) {
		super(accVer);
	}
	

	protected void setUp() throws Exception {
		//super.setIgnoreCase(true);
		 //ignorecase=true is the default
		super.setUp();
	}
	
	@Override
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/likeTest.mdb";
	}
	
	public void testLike() throws SQLException, IOException {
		checkQuery("select * from query1 order by campo2","dd1");
	}
	
	public void testLikeExternal() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE T21 (id counter primary key, descr memo)");
			st.close();
			st = super.ucanaccess.createStatement();
			
			st.execute("INSERT INTO T21 (descr)  VALUES( 'dsdsds')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'aa')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'aBa')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'aBBBa')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'PB123')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'PZ123')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'a*a')");
			st.execute("INSERT INTO T21 (descr)  VALUES( 'A*a')");
			st.execute("INSERT INTO T21 (descr)  VALUES( '#')");
			st.execute("INSERT INTO T21 (descr)  VALUES( '*')");
			Object[][] ver = { { "a*a" }, { "A*a" } };
			checkQuery(
					"select descr from T21 where descr like 'a[*]a' order by ID",
					ver);
			ver = new Object[][] { { "aa" }, { "aBa" }, { "aBBBa" }, { "a*a" },
					{ "A*a" } };
			checkQuery(
					"select descr from T21 where descr like \"a*a\" ORDER BY ID",
					ver);
			ver = new Object[][]{{2,"aa"},{3,"aBa"},{4,"aBBBa"},{7,"a*a"},{8,"A*a"}};
			checkQuery("select * from T21 where descr like 'a%a'",ver);
			
			checkQuery("select descr from T21 where descr like 'P[A-F]###'",
					"PB123");
			checkQuery("select descr from T21 where descr like 'P[!A-F]###'",
					"PZ123");
			checkQuery("select * from T21 where descr='aba'",3,"aBa");
			
		} finally {
			if (st != null)
				st.close();
		}
	}
}
