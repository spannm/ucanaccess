package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

class ReloadPersistentMirrorTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testReloadMirror(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        File dbFile = createTempFileName(getClass().getSimpleName(), getFileFormat().getFileExtension());
        dbFile.deleteOnExit();
        File mirrorFile = createTempFileName(getClass().getSimpleName(), "");
        mirrorFile.deleteOnExit();

        // create the database
        String urlCreate = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath()
            + ";memory=true" + ";newDatabaseVersion=" + getFileFormat().name();
        
        try (Connection conn = DriverManager.getConnection(urlCreate, "", "");
            Statement stCreate = conn.createStatement()) {
            stCreate.execute("CREATE TABLE Table1 (ID COUNTER PRIMARY KEY, TextField TEXT(50))");
        }
        // create the persistent mirror
        String urlMirror =
                UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";keepMirror=" + mirrorFile.getAbsolutePath();
        try (Connection conn = DriverManager.getConnection(urlMirror, "", "")) {
        }

        // do an update without the mirror involved
        String urlUpdate = UcanaccessDriver.URL_PREFIX + dbFile.getAbsolutePath() + ";memory=true";
        try (Connection conn = DriverManager.getConnection(urlUpdate, "", "");
            Statement stUpdate = conn.createStatement()) {
            stUpdate.executeUpdate("INSERT INTO Table1 (TextField) VALUES ('NewStuff')");
        }

        try (// now try and open the database with the (outdated) mirror
        Connection conn = DriverManager.getConnection(urlMirror, "", "");
            Statement stSelect = conn.createStatement();
            ResultSet rs = stSelect.executeQuery("SELECT COUNT(*) AS n FROM Table1")) {
            rs.next();
            assertEquals(1, rs.getInt(1), "Unexpected record count");
        }
    }

}
