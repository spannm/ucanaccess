package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

class ReloadPersistentMirrorTest extends UcanaccessTestBase {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testReloadMirror(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        Connection conn = null;

        File dbFile = File.createTempFile("mirrorTest", getFileFormat().getFileExtension(), TEST_DB_TEMP_DIR);
        dbFile.delete();
        File mirrorFile = File.createTempFile("mirrorTest", "", TEST_DB_TEMP_DIR);
        mirrorFile.delete();

        // create the database
        String urlCreate =
                UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";memory=true" + ";newDatabaseVersion=" + getFileFormat().name();
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
        assertEquals(1, rs.getInt(1), "Unexpected record count");
        stSelect.close();
        conn.close();

    }
}
