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
import java.sql.CallableStatement;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class ParametersTest extends AccessVersionAllTest {

    public ParametersTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/Parameters.accdb";
    }

    @Test
    public void testParameters() throws SQLException, IOException {
        dumpQueryResult("SELECT * FROM tq");
        dumpQueryResult("SELECT * FROM z");
        dumpQueryResult("SELECT * FROM [queryWithParameters]");
        dumpQueryResult("SELECT * FROM table(queryWithParameters(#1971-03-13#,'hi babe'))");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", 3);
        CallableStatement cs = ucanaccess.prepareCall("{call Insert_from_select_abxxx(?,?,?)}");
        cs.setString(1, "2");
        cs.setString(2, "YeaH!!!!");
        cs.setString(3, "u can see it works");
        cs.executeUpdate();

        dumpQueryResult("SELECT * FROM [ab\"\"\"xxx]");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", 6);
        // metaData();
        cs = ucanaccess.prepareCall("{call InsertWithFewParameters(?,?,?)}");

        cs.setString(1, "555");
        cs.setString(2, "YeaH!ddd!!!");
        cs.setDate(3, new java.sql.Date(System.currentTimeMillis()));
        cs.executeUpdate();
        cs.executeUpdate();
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [table 1]");

        dumpQueryResult("SELECT * FROM Membership");
        // test saved UPDATE query with PARAMETERS
        cs = ucanaccess.prepareCall("{call UpdateMembershipLevel(?,?)}");
        cs.setString(1, "Gold");
        cs.setInt(2, 1);
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM Membership");
        // same again, but with space after the procedure name
        cs = ucanaccess.prepareCall("{call UpdateMembershipLevel (?,?)}");
        cs.setString(1, "Platinum");
        cs.setInt(2, 1);
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM Membership");

        cs = ucanaccess.prepareCall("{call InsertNewMembership(?,?,?)}");
        cs.setString(1, "Thompson");
        cs.setString(2, "Gord");
        cs.setString(3, "Basic");
        cs.executeUpdate();
        checkQuery("SELECT @@IDENTITY", 2); // verify that we can retrieve the AutoNumber ID
        cs.executeUpdate();
        checkQuery("SELECT @@IDENTITY", 3); // and again, just to be sure
        cs = ucanaccess.prepareCall("{call UpdateWhere(?,?)}");
        cs.setString(1, "updated");
        cs.setString(2, "3x");
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [table 1]");
    }

}
