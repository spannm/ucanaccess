package net.ucanaccess.jdbc;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Disabled
class HsqlDb272Test extends UcanaccessBaseTest {

    @Test
    void testUCanAccessAndHsqldb() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ucanaccess://" + getDatabaseFile() + ";newDatabaseVersion=V2016;immediatelyReleaseResources=true");

             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE Tbl (Txt VARCHAR(42) NOT NULL PRIMARY KEY)");
            stmt.executeUpdate("INSERT INTO Tbl (Txt) VALUES ('Insert')");

            String sqlUpdate = "UPDATE Tbl SET Txt = 'Update'";

            UcanaccessSQLException ex = assertThrows(UcanaccessSQLException.class, () -> stmt.executeUpdate(sqlUpdate));
            assertInstanceOf(NullPointerException.class, ex.getCause());
        }
    }

    @Test
    void testHsqldbOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:hsqldb:file:" + getDatabaseFile() + ";hsqldb.log_data=false;shutdown=true");
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE Tbl (Txt VARCHAR(42) NOT NULL PRIMARY KEY)");
            Class<? extends org.hsqldb.Trigger> trigger = net.ucanaccess.triggers.TriggerUpdate.class;
            stmt.executeUpdate("CREATE TRIGGER triggerUpdate_Tbl AFTER UPDATE ON Tbl FOR EACH ROW CALL \"" + trigger.getName() + "\"");
            stmt.executeUpdate("INSERT INTO Tbl (Txt) VALUES ('Insert')");
            stmt.executeUpdate("UPDATE Tbl SET Txt = 'Update'");
        }
    }

    String getDatabaseFile() {
        return new File(getTestTempDir(), getShortTestMethodName()).getAbsolutePath();
    }

    @AfterEach
    void cleanUp() throws Exception {
        Optional.ofNullable(getTestTempDir().listFiles()).map(Arrays::asList).orElse(List.of())
            .stream()
            .filter(f -> f.getName().startsWith(getShortTestMethodName()))
            .forEach(File::delete);
    }

}
