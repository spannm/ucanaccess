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
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class AggregateFunctionsTest extends AccessVersionAllTest {

    public AggregateFunctionsTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE t235 (id INTEGER,descr text(400), num numeric(12,3), date0 datetime) ",
                "INSERT INTO t235 (id,descr,num,date0)  VALUES( 1234,'Show must go off',-1110.55446,#11/22/2003 10:42:58 PM#)",
                "INSERT INTO t235 (id,descr,num,date0)  VALUES( 12344,'Show must go up and down',-113.55446,#11/22/2006 10:42:58 PM#)");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("t235");
    }

    @Test
    public void testDCount() throws SQLException, IOException, ParseException {

        checkQuery("SELECT id  , DCount('*','t235','1=1') from [t235]", new Object[][] { { 1234, 2 }, { 12344, 2 } });
        checkQuery("SELECT id as [WW \"SS], DCount('descr','t235','1=1')from t235",
                new Object[][] { { 1234, 2 }, { 12344, 2 } });
        checkQuery("SELECT  DCount('*','t235','1=1') ", 2);

    }

    @Test
    public void testDSum() throws SQLException, IOException, ParseException {
        checkQuery("SELECT DSum('id','t235','1=1') ", 13578);
    }

    @Test
    public void testDMax() throws SQLException, IOException, ParseException {
        checkQuery("SELECT  DMax('id','t235') ", 12344);
    }

    @Test
    public void testDMin() throws SQLException, IOException, ParseException {
        checkQuery("SELECT  DMin('id','t235') ", 1234);
    }

    @Test
    public void testDAvg() throws SQLException, IOException, ParseException {
        checkQuery("SELECT  DAvg('id','t235') ", 6789);
    }

    @Test
    public void testLast() throws SQLException, IOException, ParseException {
        checkQuery("SELECT last(descr) from t235", "Show must go up and down");
        checkQuery("SELECT last(NUM) from t235", -113.5540);
        dumpQueryResult("SELECT last(date0) from t235");
    }

    @Test
    public void testFirst() throws SQLException, IOException, ParseException {
        checkQuery("SELECT first(descr) from t235", "Show must go off");
        checkQuery("SELECT first(NUM) from t235", -1110.5540);
        dumpQueryResult("SELECT  first(date0) from t235");
    }

    @Test
    public void testDLast() throws SQLException, IOException, ParseException {
        checkQuery("SELECT DLast('descr','t235') ", "Show must go up and down");
    }

    @Test
    public void testDFirst() throws SQLException, IOException, ParseException {
        checkQuery("SELECT DFirst('descr','t235') ", "Show must go off");
    }

}
