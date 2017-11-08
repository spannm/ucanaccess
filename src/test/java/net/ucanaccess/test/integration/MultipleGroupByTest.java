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
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class MultipleGroupByTest extends AccessVersionAllTest {

    public MultipleGroupByTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testMultiple() throws SQLException, IOException {
        Connection conn = getUcanaccessConnection();
        String wCreateTable = "CREATE TABLE TXXX (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, F4 VARCHAR, VAL NUMBER)";
        conn.createStatement().executeUpdate(wCreateTable);
        wCreateTable =
                "CREATE TABLE TABLEXXX_KO (F1,F2,VAL) AS  (SELECT F1 , F2 , SUM(VAL) FROM TXXX GROUP BY F1,F2) WITH DATA";
        conn.createStatement().executeUpdate(wCreateTable);

        conn.close();
    }
}
