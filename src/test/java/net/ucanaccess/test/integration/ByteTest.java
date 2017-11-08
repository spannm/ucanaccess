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
import java.text.ParseException;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class ByteTest extends AccessVersionAllTest {

    public ByteTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements(
            "CREATE TABLE tblMain (ID int NOT NULL PRIMARY KEY,company TEXT NOT NULL, Closed byte); ",
            "INSERT INTO tblMain (id,company) VALUES(1, 'pippo')",
            "UPDATE tblMain SET closed=255");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("tblMain");
    }

    @Test
    public void testCreate() throws SQLException, IOException, ParseException {
        dumpQueryResult("SELECT * FROM tblMain");
        checkQuery("SELECT * FROM tblMain");
    }

}
