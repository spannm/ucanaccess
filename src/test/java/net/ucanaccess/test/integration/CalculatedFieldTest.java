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
import net.ucanaccess.test.util.AccessVersion2010Test;

@RunWith(Parameterized.class)
public class CalculatedFieldTest extends AccessVersion2010Test {

    public CalculatedFieldTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/calculatedField.accdb";
    }

    @Test
    public void testFunctionBuiltInCall() throws Exception {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("INSERT INTO T (c1) VALUES ('my')");
        st.execute("INSERT INTO T (c1) VALUES ('myc')");
        st.execute("INSERT INTO T (c1) VALUES ('mycat')");
        st.execute("INSERT INTO T (c1) VALUES ('mycattom')");
        st.execute("INSERT INTO T (c1) VALUES (null)");
        dumpQueryResult("SELECT * FROM T");
        checkQuery("select c2,c3,c4,c5 from T order by id",
                new String[][] { { "my", "my", "my", "my" }, { "myc", "myc", "myc", "myc" },
                        { "myc", "myc", "cat", "cat" }, { "myc", "myc", "tom", "tom" },
                        { null, null, null, null } });

        st.close();
    }

    @Test
    public void testCalculatedFieldNameContainsPercentSign() throws Exception {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO Product (wholesale, retail) VALUES (4, 5)");
        ResultSet rs = st.executeQuery("SELECT wholesale, retail, [%markup] FROM Product WHERE [ID]=3");
        rs.next();
        assertEquals(25.0, rs.getDouble("%markup"), 0.000001d);
        st.close();
    }

}
