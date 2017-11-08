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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class BatchTest extends AccessVersionAllTest {

    public BatchTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE Tb (id LONG,name TEXT, age LONG)", "INSERT INTO Tb VALUES(1,'Sophia', 33)");
    }

    @After
    public void dropTable() throws Exception {
        dropTable("Tb");
    }

    @Test
    public void testBatch() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.addBatch("UPDATE Tb SET [name]='ccc'");
        st.addBatch("UPDATE Tb SET age=95");
        st.executeBatch();
        checkQuery("SELECT * FROM tb");
        st.close();
    }

    @Test
    public void testBatchPS() throws SQLException, IOException {
        PreparedStatement st = ucanaccess.prepareStatement("UPDATE Tb SET [name]=?,age=? ");

        st.setString(1, "ciao");
        st.setInt(2, 23);
        st.addBatch();
        st.setString(1, "ciao1");
        st.setInt(2, 43);
        st.addBatch();
        st.executeBatch();
        checkQuery("SELECT * FROM tb");
        st.close();
    }

}
