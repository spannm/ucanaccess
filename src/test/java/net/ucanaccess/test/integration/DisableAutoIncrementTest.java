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
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class DisableAutoIncrementTest extends AccessVersionDefaultTest {

    public DisableAutoIncrementTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE CT (id COUNTER PRIMARY KEY ,descr TEXT) ",
                "CREATE TABLE [C T] (id COUNTER PRIMARY KEY ,descr TEXT) ");
    }

    @Test
    public void testGuid() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE CT1 (id guid PRIMARY KEY ,descr TEXT) ");
        st.execute("INSERT INTO CT1 (descr) VALUES ('CIAO')");

        checkQuery("SELECT * FROM CT1");
        st.close();
    }

    @Test
    public void testDisable() throws SQLException, IOException {
        Statement st = null;
        boolean exc = false;
        st = ucanaccess.createStatement();
        st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        st.execute("DISABLE AUTOINCREMENT ON CT ");
        try {
            st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        } catch (Exception e) {
            exc = true;
        }
        assertTrue(exc);
        st.execute("enable AUTOINCREMENT ON CT ");
        st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        st.execute("DISABLE AUTOINCREMENT ON[C T]");
        st.close();
    }

}
