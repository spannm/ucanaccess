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

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class NoRomanCharacterTest extends AccessVersionAllTest {

    public NoRomanCharacterTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Test
    public void testNoRomanCharactersInColumnName() throws Exception {
        dumpQueryResult("SELECT * FROM NOROMAN");

        Statement st = ucanaccess.createStatement();

        st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        st.execute("UPDATE NOROMAN SET  [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
        ResultSet rs = st.executeQuery("SELECT * FROM NOROMAN");
        while (rs.next()) {
            getLogger().info(rs.getString("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß"));
        }

        checkQuery("SELECT * FROM NOROMAN");
        st.close();
    }
}
