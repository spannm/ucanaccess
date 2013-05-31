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

import com.healthmarketscience.jackcess.Database.FileFormat;

public class PivotTest extends UcanaccessTestBase {
	
	public PivotTest() {
		super();
	}
	
	public PivotTest(FileFormat accVer) {
		super(accVer);
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
	}
	
	@Override
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/pivot.mdb";
	}
	
	
	public void testPivot() throws SQLException, IOException, ParseException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			dump("select * from Table1_trim");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE',1234.56,#2003-12-03#   )");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE MIO',134.46,#2003-12-03#   )");
			st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('STA IN FRUNTE A MEEE',1344.46,#2003-12-05#   )");
			super.initVerifyConnection();
			dump("select * from Table1_trim");
			checkQuery("select * from Table1_trim");
		} finally {
			if (st != null)
				st.close();
		}
	}
}
