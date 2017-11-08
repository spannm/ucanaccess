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
public class RegexTest extends AccessVersionDefaultTest {

    public RegexTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE reg (id COUNTER,descr memo) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("reg");
    }

    @Test
    public void testRegex() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        String s = "";
        for (int i = 0; i < 5000; i++) {
            s += "C";
        }

        String[] in = new String[] { "", "\"\"'tCC", s, s + "'", s + "\"", s + "\"''t", "\"'\"t" + s,
                "ss\"1234567890wwwwwwwwww1", "ssss'DDDD", s + "\"\"\"" + s };
        for (String c : in) {
            executeStatement(c);
        }
        String[][] out = new String[in.length * 2][1];
        int k = 0;
        for (int j = 0; j < out.length; j++) {
            out[j][0] = in[k];
            if (j % 2 == 1) {
                k++;
            }
        }
        checkQuery("SELECT descr FROM reg ORDER BY id ASC", out);

        st.close();
    }

    private void executeStatement(String s) throws SQLException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute(getStatement(s.replaceAll("'", "''"), "'"));

            st.execute(getStatement(s.replaceAll("\"", "\"\""), "\""));

        } catch (SQLException sqle) {
            System.err.println(getStatement(s, "\""));
            System.err
                    .println("converted sql: " + ucanaccess.nativeSQL(getStatement(s.replaceAll("\"", "\"\""), "\"")));
            throw sqle;
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    private String getStatement(String _s, String _dlm) {
        return "INSERT INTO reg (descr)  VALUES(  " + _dlm + _s + _dlm + ")";
    }

}
