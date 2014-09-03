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
