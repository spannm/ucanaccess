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
					"select descr from T21 where descr like \"a*a\"  AND '1'='1' and (descr) like \"a*a\" ORDER BY ID",
					ver);
			ver = new Object[][]{{2,"aa"},{3,"aBa"},{4,"aBBBa"},{7,"a*a"},{8,"A*a"}};
			checkQuery("select * from T21 where descr like 'a%a'",ver);
			
			checkQuery("select descr from T21 where descr like 'P[A-F]###'",
					"PB123");
			checkQuery("select descr from T21 where (T21.descr\n) \nlike 'P[!A-F]###' AND '1'='1'",
					"PZ123");
			checkQuery("select * from T21 where descr='aba'",3,"aBa");
			
		} finally {
			if (st != null)
				st.close();
		}
	}
}
