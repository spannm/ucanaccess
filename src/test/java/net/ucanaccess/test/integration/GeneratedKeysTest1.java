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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class GeneratedKeysTest1 extends AccessVersionAllTest {
    private String tableName = "T_Key1";

    public GeneratedKeysTest1(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE " + tableName + " ( Z GUID PRIMARY KEY, B char(4) )");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable(tableName);
    }

    @Test
    public void testGeneratedKeys() throws SQLException, IOException {

        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO " + tableName + " (B) VALUES (?)");
        ps.setString(1, "");
        ps.execute();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();
        PreparedStatement ps1 = ucanaccess.prepareStatement("Select @@identity ");
        ResultSet rs1 = ps1.executeQuery();
        rs1.next();
        assertEquals(rs1.getString(1), rs.getString(1));
        ps.close();

        checkQuery("SELECT * FROM " + tableName);

    }
}
