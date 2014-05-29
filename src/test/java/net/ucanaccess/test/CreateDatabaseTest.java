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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class CreateDatabaseTest extends UcanaccessTestBase {
	public CreateDatabaseTest() {
		super();
	}
	
	public CreateDatabaseTest(FileFormat accVer) {
		super(accVer);
	}
	
	
	protected void setUp() throws Exception {}
	
	
	public void testNewDatabase() throws Exception {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		File fileMdb = new File("./"+Math.random()+".mdb");
		Connection ucanaccessConnection = null;
		try {
			String url =UcanaccessDriver.URL_PREFIX
			+ fileMdb.getAbsolutePath()+";newdatabaseversion="+FileFormat.V2003.name();
			
			ucanaccessConnection = DriverManager.getConnection(url, "", "");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNotNull(ucanaccessConnection);
		System.out.println("Mdb was successfull created @ "+fileMdb .getAbsolutePath());
		Statement st = null;
		st =ucanaccessConnection.createStatement();
		st.execute(" CREATE TABLE AAA ( baaaa text(3) PRIMARY KEY,A long default 3, C text(4) ) ");
		st.close();
		st =ucanaccessConnection.createStatement();
		st.execute("INSERT INTO AAA(baaaa,c) VALUES ('33A','G'   )");
		st.execute("INSERT INTO AAA VALUES ('33B',111,'G'   )");
		super.ucanaccess=ucanaccessConnection;
		dump("SELECT * FROM AAA");
		
	
	}
}
