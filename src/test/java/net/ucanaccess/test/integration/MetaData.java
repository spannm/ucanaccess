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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class MetaData extends AccessVersionDefaultTest {

    public MetaData(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("AAAn");
    }

    public void createSimple(String a, Object[][] ver) throws SQLException, IOException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute("INSERT INTO AAAn VALUES ('33A',11,'" + a + "'   )");
            st.execute("INSERT INTO AAAn VALUES ('33B',111,'" + a + "'    )");
            checkQuery("SELECT * FROM AAAn", ver);
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    @Test
    public void testDrop() throws SQLException, IOException {
        Statement st = null;
        ucanaccess.setAutoCommit(false);
        createSimple("a", new Object[][] { { "33A", 11, "a" }, { "33B", 111, "a" } });
        st = ucanaccess.createStatement();
        st.executeUpdate("DROP TABLE AAAn");

        st.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
        createSimple("b", new Object[][] { { "33A", 11, "b" }, { "33B", 111, "b" } });

        ucanaccess.commit();
        st.close();
    }
}
