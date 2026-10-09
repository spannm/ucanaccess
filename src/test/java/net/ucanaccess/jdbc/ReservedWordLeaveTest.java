package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.System.Logger.Level;
import java.sql.ResultSet;
import java.sql.SQLException;

class ReservedWordLeaveTest extends UcanaccessBaseFileTest {

    @Test
    void testLoadReserved() throws SQLException {
        init();
        checkQuery("SELECT COUNT(LEAVE) FROM t_leave", singleRec(0));
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
                assertLeftRow(st, tbl);
            }
        }

        UcanaccessConnectionBuilder builderExisting = buildConnection()
            .withDbPath(fileMdb.getAbsolutePath())
            .withImmediatelyReleaseResources();

        try (UcanaccessConnection conn = builderExisting.build()) {

            try (UcanaccessStatement st = conn.createStatement()) {
                assertLeftRow(st, tbl);
                st.execute("DROP TABLE " + tbl);
            }
        }

    }

    private static void assertLeftRow(UcanaccessStatement st, String tbl) throws SQLException {
        try (ResultSet rs = st.executeQuery("SELECT LEAVE FROM " + tbl)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("left");
            assertThat(rs.next()).isFalse();
        }
    }

}
