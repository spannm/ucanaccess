/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
