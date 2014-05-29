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
import java.sql.ResultSet;
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;


public class ExternalResourcesTest extends UcanaccessTestBase {
	public ExternalResourcesTest() {
		super();
	}
	
	public ExternalResourcesTest(FileFormat accVer) {
		super(accVer);
	}
	
	
	protected void setUp() throws Exception {}
	
	
	public void testLinks() throws Exception {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		Connection conn = null;
		Statement st = null;
		File main=super.copyResourceInTemp("net/ucanaccess/test/resources/main.mdb");
		File linkee1=super.copyResourceInTemp("net/ucanaccess/test/resources/linkee1.mdb");
		File linkee2=super.copyResourceInTemp("net/ucanaccess/test/resources/linkee2.mdb");
		try {
		
		String url=UcanaccessDriver.URL_PREFIX
				+ main.getAbsolutePath()+";remap=c:\\db\\linkee1.mdb|"+linkee1.getAbsolutePath()+"&c:\\db\\linkee2.mdb|"+linkee2.getAbsolutePath();
		System.out.println(url);
		conn=DriverManager.getConnection(url, "", "");
		st = conn.createStatement();
			ResultSet rs=st.executeQuery("select * from table1");
			dump(rs);
			rs=st.executeQuery("select * from table2");
			dump(rs);
		} finally {
			if (st != null)
				st.close();
			if (conn != null)
				conn.close();
		}
	}
}
