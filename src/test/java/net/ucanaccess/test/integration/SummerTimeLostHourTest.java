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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

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
    public void testForLostHour() throws SQLException, IOException {
        /*
         * ensure that #2017-03-26 02:00:00# doesn't "magically" 
         *      become #2017-03-26 01:00:00# when written to HSQLDB
         */
        Connection hsqldbConn = ucanaccess.getHSQLDBConnection();
        Statement hsqldbStmt = hsqldbConn.createStatement();
        ResultSet rs = hsqldbStmt.executeQuery("SELECT CAST(DTMFIELD AS VARCHAR(26)) AS str FROM TABLE1 WHERE ID=1");
        rs.next();
        assertEquals("2017-03-26 02:00:00.000000", rs.getString(1));

        /*
         * also ensure that 02:00:00 -> 01:00:00 doesn't happen when writing back to Access
         */
        Statement ucaStmt = ucanaccess.createStatement();
        ucaStmt.executeUpdate("UPDATE Table1 SET txtField='updated' WHERE id=1");
        //
        TimeZone tzUtc = TimeZone.getTimeZone("UTC");
        //
        Calendar expectedBackFromAccess = Calendar.getInstance(tzUtc);
        expectedBackFromAccess.clear();
        expectedBackFromAccess.set(2017, 2, 26, 2, 0, 0);
        //
        Database db = ucanaccess.getDbIO();
        TimeZone prevJackcessTimeZone = db.getTimeZone();
        db.setTimeZone(tzUtc);
        Table tbl = db.getTable("Table1");
        Row r = CursorBuilder.findRowByPrimaryKey(tbl, 1);
        assertEquals(expectedBackFromAccess.getTimeInMillis(), r.getDate("dtmField").getTime());
        //
        db.setTimeZone(prevJackcessTimeZone);
    }

}
