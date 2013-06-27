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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class AliasTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public AliasTest() {
		super();
	}
	
	public AliasTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE Talias (id LONG,descr memo) ");
		
	}
	
	public void testBig() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			int id = 6666554;
			st.execute("INSERT INTO Talias (id,descr)  VALUES( " + id
					+ ",'t')");
			ResultSet rs=st.executeQuery("select descr as [cipol%'&la]  from Talias " +
					" where descr<>'ciao'&'bye'&'pippo'");
			rs.next();
			System.out.println(rs.getMetaData().getColumnLabel(1));
			System.out.println(rs.getObject("cipol%'&la"));
			
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
