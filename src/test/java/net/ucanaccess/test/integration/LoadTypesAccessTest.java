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
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class LoadTypesAccessTest extends AccessVersionAllTest {
    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public LoadTypesAccessTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements(
                "CREATE TABLE pluto (id LONG, descr MEMO, dt DATETIME,euros CURRENCY,float1 SINGLE, double1 DOUBLE, int1 INTEGER,numeric0 numeric(24,5), numeric1 double) ",
                "INSERT INTO pluto (id,descr,dt,euros,float1,double1,int1,numeric0,numeric1 )  VALUES( 1234,'I like trippa with spaghettis bolognese',#10/03/2008 10:34:35 PM#,4.55555,5.6666,6.7,5,0.100051,4.677856)");
    }

    @Test
    public void testDate() throws SQLException, IOException, ParseException {
        checkQuery(
                "SELECT #10/03/2004# , #11/23/1811#,#10/03/2008 22:34:35#,#10/03/2008 22:34:35 aM#,#10/03/2008 10:34:35 PM# from pluto",
                SDF.parse("2004-10-03 00:00:00.0"), SDF.parse("1811-11-23 00:00:00.0"),
                SDF.parse("2008-10-03 22:34:35.0"), SDF.parse("2008-10-03 22:34:35.0"),
                SDF.parse("2008-10-03 22:34:35.0"));
        checkQuery("SELECT #22:34:35#,#10:34:35 AM#,#10:34:35 pM# from pluto", SDF.parse("1899-12-30 22:34:35.0"),
                SDF.parse("1899-12-30 10:34:35.0"), SDF.parse("1899-12-30 22:34:35.0"));
    }

    @Test
    public void testQuery() throws SQLException, IOException, ParseException {
        checkQuery("SELECT * FROM pluto", 1234, "I like trippa with spaghettis bolognese",
                SDF.parse("2008-10-03 22:34:35"), 4.5555, 5.6666, 6.7, 5, 0.10005, 4.677856);
    }
}
