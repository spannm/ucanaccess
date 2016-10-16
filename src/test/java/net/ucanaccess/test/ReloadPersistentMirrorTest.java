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

import org.junit.Assert;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;


public class ReloadPersistentMirrorTest extends UcanaccessTestBase {
	public ReloadPersistentMirrorTest() {
		super();
	}
	
	public ReloadPersistentMirrorTest(FileFormat accVer) {
		super(accVer);
	}
	
	
	
	public void testReloadMirror() throws Exception {
		Connection conn = null;
		Statement stCreate = null;
		Statement stUpdate = null;
		Statement stSelect = null;
		ResultSet rs = null;
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
					";keepMirror=" + mirrorFile.getAbsolutePath() +
					";immediatelyReleaseResources=true";
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
			stSelect = conn.createStatement();
			rs = stSelect.executeQuery("SELECT COUNT(*) AS n FROM Table1");
			rs.next();
			Assert.assertEquals(1, rs.getInt(1));
			conn.close();
		} finally {
			if (stCreate != null)
				stCreate.close();
			if (stUpdate != null)
				stUpdate.close();
			if (stSelect != null)
				stSelect.close();
			if (conn != null)
				conn.close();
		}
	}
}
