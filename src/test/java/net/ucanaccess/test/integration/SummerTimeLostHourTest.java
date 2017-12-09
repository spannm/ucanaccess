package net.ucanaccess.test.integration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2007Test;

@RunWith(Parameterized.class)
// TODO: enable this once we have a fix for Ticket_5
@Ignore
public class SummerTimeLostHourTest extends AccessVersion2007Test {

    private static Locale   prevLocale;
    private static TimeZone prevTimeZone;

    @BeforeClass
    public static void goToRome() {
        prevLocale = Locale.getDefault();
        prevTimeZone = TimeZone.getDefault();
        Locale.setDefault(Locale.ITALY);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Rome"));
    }

    @AfterClass
    public static void returnHomeFromRome() {
        Locale.setDefault(prevLocale);
        TimeZone.setDefault(prevTimeZone);
    }

    public SummerTimeLostHourTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/SummerTimeLostHour.accdb"; // Access 2007
    }

    @Test
    public void testForLostHour() throws SQLException {
        /*
         * ensure that #2017-03-26 02:00:00# doesn't "magically" 
         *      become #2017-03-26 01:00:00#
         */
        Connection hsqldbConn = ucanaccess.getHSQLDBConnection();
        Statement s = hsqldbConn.createStatement();
        ResultSet rs = s.executeQuery("SELECT CAST(DTMFIELD AS VARCHAR(26)) AS str FROM TABLE1 WHERE ID=1");
        rs.next();
        assertEquals("2017-03-26 02:00:00.000000", rs.getString(1));
    }

}
