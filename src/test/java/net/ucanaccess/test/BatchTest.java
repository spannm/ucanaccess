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
		 executeCreateTable("CREATE TABLE T1 (id LONG,name TEXT, age LONG) ");
		 
		Statement	st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO T1 VALUES(1,'Sophia', 33)");

			st.close();
		
	}
	
	public void testBatch() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.addBatch("UPDATE T1 SET [name]='ccc'");
			st.addBatch("UPDATE T1 SET age=95");
			st.executeBatch();
			checkQuery("select * from t1");
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testBatchPS() throws SQLException, IOException {
		PreparedStatement st = null;
		try {
			st = super.ucanaccess.prepareStatement("UPDATE T1 SET [name]=?,age=? ");
			
			st.setString(1, "ciao");
			st.setInt(2, 23);
			st.addBatch();
			st.setString(1, "ciao1");
			st.setInt(2, 43);
			st.addBatch();
			st.executeBatch();
			checkQuery("select * from t1");
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
