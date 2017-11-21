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
package net.ucanaccess.test.integration;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class ReloadPersistentMirrorTest extends AccessVersionAllTest {

    public ReloadPersistentMirrorTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testReloadMirror() throws Exception {
        Connection conn = null;

        File dbFile = File.createTempFile("mirrorTest", fileFormat.getFileExtension(), TEST_DB_TEMP_DIR);
        dbFile.delete();
        File mirrorFile = File.createTempFile("mirrorTest", "", TEST_DB_TEMP_DIR);
        mirrorFile.delete();

        // create the database
        String urlCreate =
                UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";memory=true" + ";newDatabaseVersion=" + fileFormat.name();
        conn = DriverManager.getConnection(urlCreate, "", "");
        Statement stCreate = conn.createStatement();
        stCreate.execute("CREATE TABLE Table1 (ID COUNTER PRIMARY KEY, TextField TEXT(50))");
        stCreate.close();
        conn.close();

        // create the persistent mirror
        String urlMirror =
                UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";keepMirror=" + mirrorFile.getAbsolutePath();
        conn = DriverManager.getConnection(urlMirror, "", "");
        conn.close();

        // do an update without the mirror involved
        String urlUpdate = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";memory=true";
        conn = DriverManager.getConnection(urlUpdate, "", "");
        Statement stUpdate = conn.createStatement();
        stUpdate.executeUpdate("INSERT INTO Table1 (TextField) VALUES ('NewStuff')");
        stUpdate.close();
        conn.close();

        // now try and open the database with the (outdated) mirror
        conn = DriverManager.getConnection(urlMirror, "", "");
        Statement stSelect = conn.createStatement();
        ResultSet rs = stSelect.executeQuery("SELECT COUNT(*) AS n FROM Table1");
        rs.next();
        Assert.assertEquals("Unexpected record count.", 1, rs.getInt(1));
        stSelect.close();
        conn.close();

    }
}
