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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class ExternalResourcesTest extends AccessVersionDefaultTest {

    public ExternalResourcesTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testLinks() throws Exception {
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        File main = copyResourceToTempFile("testdbs/main.mdb");
        File linkee1 = copyResourceToTempFile("testdbs/linkee1.mdb");
        File linkee2 = copyResourceToTempFile("testdbs/linkee2.mdb");
        String url = UcanaccessDriver.URL_PREFIX + main.getAbsolutePath() + ";immediatelyreleaseresources=true"
                + ";remap=c:\\db\\linkee1.mdb|" + linkee1.getAbsolutePath() + "&c:\\db\\linkee2.mdb|"
                + linkee2.getAbsolutePath();
        getLogger().info("Database url: {}", url);
        Connection conn = DriverManager.getConnection(url, "", "");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT * FROM table1");
        dumpQueryResult(rs);
        rs = st.executeQuery("SELECT * FROM table2");
        dumpQueryResult(rs);
        rs.close();
        st.close();
        conn.close();
    }
}
