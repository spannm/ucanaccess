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

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class UnproperExecuteQueryTest extends AccessVersionAllTest {

    public UnproperExecuteQueryTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Test
    public void testExecute() throws Exception {
        execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        execute("UPDATE NOROMAN SET [ENd]='BLeah'");
        execute("delete from NOROMAN");
    }

    private void execute(String s) throws SQLException {
        Statement st = ucanaccess.createStatement();
        try {
            st.executeQuery(s);
            fail("Should not get here");
        } catch (Exception e) {
            // e.printStackTrace();
            getLogger().info(e.getMessage());
        }
        st.execute(s);
    }
}
