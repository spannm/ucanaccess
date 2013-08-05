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

public class InsertBigTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public InsertBigTest() {
		super();
	}
	
	public InsertBigTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE Tbig (id LONG,descr memo) ");
		
	}
	
	public void testBig() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			int id = 6666554;
			String s="t";
			for(int i=0;i<10000;i++){
				s+="t\n";
			}
			st.execute("INSERT INTO Tbig (id,descr)  VALUES( " + id
					+ ",'"+s+"')");
			dump("select * from Tbig");
			
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	
}
