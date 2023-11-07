package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.TimeZone;

class UtcTimezoneTest extends UcanaccessBaseTest {

    private static TimeZone prevTimeZone;

    @BeforeAll
    static void setLocalAndTimezone() {
        prevTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Rome"));
    }

    @AfterAll
    static void resetLocalAndTimezone() {
        TimeZone.setDefault(prevTimeZone);
    }

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "utcTimezoneTest.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testForLostHour(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        /*
         * ensure that #2017-03-26 02:00:00# doesn't "magically"
         * become #2017-03-26 01:00:00# when written to HSQLDB
         */
        Connection hsqldbConn = ucanaccess.getHSQLDBConnection();
        Statement hsqldbStmt = hsqldbConn.createStatement();
        ResultSet rs = hsqldbStmt.executeQuery("SELECT f_datetime, CAST(f_datetime AS VARCHAR(26)) AS str FROM t_datetime WHERE id=1");
        rs.next();
        assertEquals("2017-03-26 02:00:00.000000", rs.getString("f_datetime"));
        assertEquals("2017-03-26 02:00:00.000000", rs.getString("str"));

        /*
         * also ensure that 02:00:00 -> 01:00:00 doesn't happen when writing back to Access
         */
        Statement ucaStmt = ucanaccess.createStatement();
        ucaStmt.executeUpdate("UPDATE t_datetime SET f_descr='updated' WHERE id=1");

        LocalDateTime expectedBackFromAccess = LocalDateTime.of(2017, 3, 26, 2, 0);
        Database db = ucanaccess.getDbIO();
        Table tbl = db.getTable("t_datetime");
        Row r = CursorBuilder.findRowByPrimaryKey(tbl, 1);
        assertEquals(expectedBackFromAccess, r.get("f_datetime"));
    }

}
