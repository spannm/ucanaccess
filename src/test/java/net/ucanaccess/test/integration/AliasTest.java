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
import java.sql.ResultSet;
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
public class AliasTest extends AccessVersionAllTest {

    public AliasTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE Talias (id LONG, descr memo, Actuación  text) ");
    }

    @After
    public void dropTable() throws Exception {
        dropTable("Talias");
    }

    @Test
    public void testBig() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        int id = 6666554;
        st.execute("INSERT INTO Talias (id,descr)  VALUES( " + id + ",'t')");
        ResultSet rs = st.executeQuery("select descr as [cipol%'&la]  from Talias where descr<>'ciao'&'bye'&'pippo'");
        rs.next();
        getLogger().info("columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
        getLogger().info("getObject: {}", rs.getObject("cipol%'&la"));

        st.close();
    }

    @Test
    public void testAccent() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO Talias (id,Actuación)  VALUES(1,'X')");
        ResultSet rs = st.executeQuery("select  [Actuación] as Actuació8_0_0_ from Talias ");
        rs.next();
        getLogger().info("columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
        getLogger().info("getObject: {}", rs.getObject("Actuació8_0_0_"));
        st.close();
    }

    @Test
    public void testAsin() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE xxxx (asin text, ff text)");
        dumpQueryResult("SELECT asin, ff from xxxx");
        st.close();
    }

}
