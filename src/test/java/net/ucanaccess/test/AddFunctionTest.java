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

import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class AddFunctionTest extends UcanaccessTestBase {

	
	public AddFunctionTest() {
		super();
	}
	
	public AddFunctionTest(FileFormat accVer) {
		super(accVer);
	}

	public void testAddFunction() throws Exception {
			Statement st = super.ucanaccess.createStatement();
			st.executeUpdate("CREATE TABLE gooo (id INTEGER) ");
			st.close();
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO gooo (id )  VALUES(1)");
			UcanaccessConnection uc=(UcanaccessConnection)super.ucanaccess;
			uc.addFunctions(AddFunctionClass.class);
			super.dump("select pluto('hello',' world ',  now ()) from gooo");
			checkQuery("select concat('Hello World, ','Ucanaccess') from gooo","Hello World, Ucanaccess");
	}
	
	
}



