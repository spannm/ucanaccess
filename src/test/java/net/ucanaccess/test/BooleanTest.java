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

public class BooleanTest extends UcanaccessTestBase {
	private static boolean init;

	public BooleanTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public BooleanTest(FileFormat accVer) {
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
					.executeUpdate("CREATE TABLE tblMain (ID COUNTER NOT NULL PRIMARY KEY,company TEXT NOT NULL,  Closed YESNO); ");
			st.close();
			st = super.ucanaccess.createStatement();
			st
					.executeUpdate("insert into tblMain (company)values('pippo')");
			st
			.executeUpdate("update tblMain set closed=yes");
			
			init = true;
		}

	}
	
	public void testCreate() throws SQLException, IOException, ParseException {
		dump("select * from  tblMain");
		checkQuery("select * from  tblMain");
	}

	
	
}
