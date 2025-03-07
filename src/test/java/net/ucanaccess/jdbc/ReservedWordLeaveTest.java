package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.System.Logger.Level;
import java.sql.SQLException;

class ReservedWordLeaveTest extends UcanaccessBaseFileTest {

    @Test
    void testLoadReserved() throws SQLException {
        init();
    }

    @Test
    void testCreateTable() throws SQLException {
        AccessVersion accessVersion = AccessVersion.V2010;
        File fileMdb = createTempFileName(getClass().getSimpleName(), accessVersion.getFileFormat().getFileExtension());
        fileMdb.deleteOnExit();

        UcanaccessConnectionBuilder builderNew = buildConnection()
            .withDbPath(fileMdb.getAbsolutePath())
            .withoutUserPass()
            .withImmediatelyReleaseResources()
            .withNewDatabaseVersion(accessVersion);
        getLogger().log(Level.DEBUG, "Database url: {0}", builderNew.getUrl());

        String tbl = "t_leave";

        try (UcanaccessConnection conn = builderNew.build()) {
            getLogger().log(Level.DEBUG, "Database file successfully created: {0}", fileMdb.getAbsolutePath());

            try (UcanaccessStatement st = conn.createStatement()) {
                executeStatements(st,
                    "CREATE TABLE " + tbl + " (LEAVE TEXT)",
                    "INSERT INTO " + tbl + " (LEAVE) VALUES('left')");
                dumpQueryResult(() -> st.executeQuery("SELECT * FROM " + tbl));
            }
        }

        UcanaccessConnectionBuilder builderExisting = buildConnection()
            .withDbPath(fileMdb.getAbsolutePath())
            .withImmediatelyReleaseResources();

        try (UcanaccessConnection conn = builderExisting.build()) {

            try (UcanaccessStatement st = conn.createStatement()) {
                dumpQueryResult(() -> st.executeQuery("SELECT * FROM " + tbl));
                st.execute("DROP TABLE " + tbl);
            }
        }


    }

}
