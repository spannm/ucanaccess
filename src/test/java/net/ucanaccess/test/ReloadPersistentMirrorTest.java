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
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;


public class ReloadPersistentMirrorTest extends UcanaccessTestBase {
	public ReloadPersistentMirrorTest() {
		super();
	}
	
	public ReloadPersistentMirrorTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {}
	
	public void testReloadMirror() throws Exception {
		Connection conn = null;
		Statement stCreate = null;
		Statement stUpdate = null;
		try {
			File dbFile = File.createTempFile("ucaTest", ".mdb");
			dbFile.delete();
			File mirrorFile = File.createTempFile("mirror", "");
			mirrorFile.delete();
			
			// create the database
			String urlCreate = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + 
					";memory=true" +
					";newDatabaseVersion=V2003" + 
					";immediatelyReleaseResources=true";
			conn = DriverManager.getConnection(urlCreate, "", "");
			stCreate = conn.createStatement();
			stCreate.execute("CREATE TABLE Table1 (ID COUNTER PRIMARY KEY, TextField TEXT(50))");
			conn.close();

			// create the persistent mirror
			String urlMirror = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + 
					";newDatabaseVersion=V2003" + 
					";immediatelyReleaseResources=true" +
					";keepMirror=" + mirrorFile.getAbsolutePath();
			conn = DriverManager.getConnection(urlMirror, "", "");
			conn.close();
			
			// do an update without the mirror involved
			String urlUpdate = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + 
					";memory=true" + 
					";immediatelyReleaseResources=true";
			conn = DriverManager.getConnection(urlUpdate, "", "");
			stUpdate = conn.createStatement();
			stUpdate.executeUpdate("INSERT INTO Table1 (TextField) VALUES ('NewStuff')");
			conn.close();
			
			// now try and open the database with the (outdated) mirror
			conn = DriverManager.getConnection(urlMirror, "", "");
			System.out.println("Database (with mirror) successfully re-opened.");
			conn.close();
		} finally {
			if (stCreate != null)
				stCreate.close();
			if (stUpdate != null)
				stUpdate.close();
			if (conn != null)
				conn.close();
		}
	}
}
