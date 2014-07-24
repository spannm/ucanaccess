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
import java.text.ParseException;

import java.util.Locale;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class ByteTest extends UcanaccessTestBase {
	private static boolean init;

	public ByteTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public ByteTest(FileFormat accVer) {
		super(accVer);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		if (!init) {
			Statement st = null;
		
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE tblMain (ID int NOT NULL PRIMARY KEY,company TEXT NOT NULL,  Closed byte); ");
			st.close();
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("insert into tblMain (id,company)values(1,'pippo')");
			st
			.executeUpdate("update tblMain set closed=255");
			try{
			st
			.executeUpdate("update tblMain set closed=-1");
			}catch(Exception e){}
			
		
		}

	}
	
	
	
	
	public void testCreate() throws SQLException, IOException, ParseException {
		dump("select * from  tblMain");
		checkQuery("select * from  tblMain");
		
	}
	
	
	

	
	
}
