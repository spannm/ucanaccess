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
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class DropTableTest extends AccessVersionAllTest {

    public DropTableTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ",
                "CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
    }

    public void createSimple(String _tableName, String a, Object[][] ver) throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO " + _tableName + " VALUES ('33A',11,'" + a + "'   )");
        st.execute("INSERT INTO " + _tableName + " VALUES ('33B',111,'" + a + "'    )");
        checkQuery("SELECT * FROM " + _tableName + " ORDER BY c", ver);
        st.close();
    }

    @Test
    public void testDrop() throws SQLException, IOException {
        Statement st = null;
        // ucanaccess.setAutoCommit(false);
        createSimple("AAAn", "a", new Object[][] { { "33A", 11, "a" }, { "33B", 111, "a" } });
        st = ucanaccess.createStatement();
        st.executeUpdate("DROP TABLE AAAn");
        // ucanaccess.commit();
        st.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
        createSimple("AAAn", "b", new Object[][] { { "33A", 11, "b" }, { "33B", 111, "b" } });
        dumpQueryResult("SELECT * FROM AAAn");
        ucanaccess.commit();
        st.close();
    }

    @Test
    public void testDropBlank() throws SQLException, IOException {
        Statement st = null;
        // ucanaccess.setAutoCommit(false);
        createSimple("[AAA n]", "a", new Object[][] { { "33A", 11, "a" }, { "33B", 111, "a" } });
        st = ucanaccess.createStatement();
        st.executeUpdate("DROP TABLE [AAA n]");
        // ucanaccess.commit();
        st.execute("CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
        createSimple("[AAA n]", "b", new Object[][] { { "33A", 11, "b" }, { "33B", 111, "b" } });
        dumpQueryResult("SELECT * FROM [AAA n]");
        ucanaccess.commit();
        st.close();
    }
}
