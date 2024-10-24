package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.File;
import java.lang.System.Logger.Level;
import java.sql.ResultSet;

class ReloadPersistentMirrorTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    @SuppressWarnings("PMD.UnusedLocalVariable")
    void testReloadMirror(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        File dbFile = createTempFileName(getClass().getSimpleName(), getFileExtension());
        dbFile.deleteOnExit();
        File mirrorFile = createTempFileName(getClass().getSimpleName(), "");
        mirrorFile.deleteOnExit();

        // create the database
        try (UcanaccessConnection conn = buildConnection()
                .withDbPath(dbFile.getAbsolutePath())
                .withoutUserPass()
                .withMemory()
                .withNewDatabaseVersion(getAccessVersion())
                .build();
             UcanaccessStatement stCreate = conn.createStatement()) {
            stCreate.execute("CREATE TABLE Table1 (ID COUNTER PRIMARY KEY, TextField TEXT(50))");
        }

        // create the persistent mirror
        UcanaccessConnectionBuilder mirrorBuilder = buildConnection()
            .withDbPath(dbFile.getAbsolutePath())
            .withProp(Property.keepMirror, mirrorFile.getAbsolutePath())
            .withoutUserPass();
        try (UcanaccessConnection conn = mirrorBuilder.build()) {
            getLogger().log(Level.DEBUG, "Openend and closed mirror connection");
        }

        // do an update without the mirror involved
        try (UcanaccessConnection conn = buildConnection()
                .withDbPath(dbFile.getAbsolutePath())
                .withMemory()
                .withoutUserPass().build();
             UcanaccessStatement stUpdate = conn.createStatement()) {
            stUpdate.executeUpdate("INSERT INTO Table1 (TextField) VALUES ('NewStuff')");
        }

        // now try and open the database with the (outdated) mirror
        try (UcanaccessConnection conn = mirrorBuilder.build()) {
             UcanaccessStatement stSelect = conn.createStatement();
            ResultSet rs = stSelect.executeQuery("SELECT COUNT(*) AS n FROM Table1");
            rs.next();
            assertEquals(1, rs.getInt(1), "Unexpected record count");
        }
    }

}
