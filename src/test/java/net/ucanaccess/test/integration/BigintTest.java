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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2016Test;

@RunWith(Parameterized.class)
public class BigintTest extends AccessVersion2016Test {

    public BigintTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }
    
    @Test
    public void testBigintInsert() throws Exception {
        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE table1 (entry TEXT(50) PRIMARY KEY, x BIGINT)");
        Long expected = 3000000000L;
        String sql = String.format("INSERT INTO table1 (entry, x) VALUES ('3 billion', %d)", expected);
        st.execute(sql);
        st.close();
        ucanaccess.close();
        String connUrl = UcanaccessDriver.URL_PREFIX + accdbPath + ";immediatelyReleaseResources=true";
        Connection cnxn = DriverManager.getConnection(connUrl);
        st = cnxn.createStatement();
        ResultSet rs = st.executeQuery("SELECT x FROM table1 WHERE entry='3 billion'");
        rs.next();
        Long actual = rs.getLong("x");
        assertEquals(expected, actual);
        cnxn.close();
    }
}
