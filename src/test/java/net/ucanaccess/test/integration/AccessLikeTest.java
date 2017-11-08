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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class AccessLikeTest extends AccessVersionAllTest {

    public AccessLikeTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/likeTest.mdb";
    }

    @Test
    public void testLike() throws SQLException, IOException {
        checkQuery("SELECT * FROM query1 order by campo2", "dd1");
    }

    @Test
    public void testLikeExternal() throws SQLException, IOException {
        String tableName = "T21";
        Statement st;

        st = ucanaccess.createStatement();
        st.executeUpdate("CREATE TABLE " + tableName + " (id counter primary key, descr memo)");
        st.close();
        st = ucanaccess.createStatement();

        st.execute("INSERT INTO T21 (descr)  VALUES( 'dsdsds')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'aa')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'aBa')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'aBBBa')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'PB123')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'PZ123')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'a*a')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'A*a')");
        st.execute("INSERT INTO T21 (descr)  VALUES( 'ss#sss')");
        st.execute("INSERT INTO T21 (descr)  VALUES( '*')");
        st.execute("INSERT INTO T21 (descr)  VALUES( '132B')");
        st.execute("INSERT INTO T21 (descr)  VALUES( '138')");
        st.execute("INSERT INTO T21 (descr)  VALUES( '138#')");
        Object[][] ver = { { "a*a" }, { "A*a" } };
        checkQuery("SELECT descr FROM T21 where descr like 'a[*]a' order by ID", ver);
        ver = new Object[][] { { "aa" }, { "aBa" }, { "aBBBa" }, { "a*a" }, { "A*a" } };

        checkQuery("SELECT descr FROM T21 where descr like \"a*a\"  AND '1'='1' and (descr) like \"a*a\" ORDER BY ID",
                ver);
        ver = new Object[][] { { 2, "aa" }, { 3, "aBa" }, { 4, "aBBBa" }, { 7, "a*a" }, { 8, "A*a" } };
        checkQuery("SELECT * FROM T21 where descr like 'a%a'", ver);

        checkQuery("SELECT descr FROM T21 where descr like 'P[A-F]###'", "PB123");
        checkQuery("SELECT descr FROM T21 where (T21.descr\n) \nlike 'P[!A-F]###' AND '1'='1'", "PZ123");
        checkQuery("SELECT * FROM T21 where descr='aba'", 3, "aBa");
        checkQuery("SELECT descr FROM T21 where descr like '13[1-4][A-F]'", "132B");
        checkQuery("SELECT descr FROM T21 where descr like '13[!1-4]'", "138");
        checkQuery("SELECT descr FROM T21 where descr like '%s[#]%'", "ss#sss");
        checkQuery("SELECT descr FROM T21 where descr like '###'", "138");
        checkQuery("SELECT descr FROM T21 where descr like '###[#]'", "138#");

        checkQuery("SELECT descr FROM T21 where descr like '###[#]'", "138#");
        checkQuery("SELECT descr FROM T21 where (( descr like '###[#]'))", "138#");
        st.close();

        dropTable(tableName);
    }

    @Test
    public void testNotLikeExternal() throws SQLException, IOException {
        String tableName = "Tx21";
        Statement st;

        st = ucanaccess.createStatement();
        st.executeUpdate("CREATE TABLE " + tableName + " (id counter primary key, descr memo)");
        st.close();

        st = ucanaccess.createStatement();
        st.execute("INSERT INTO Tx21 (descr)  VALUES( 't11114')");
        st.execute("INSERT INTO Tx21 (descr)  VALUES( 't1111C')");
        st.execute("INSERT INTO Tx21 (descr)  VALUES( 't1111')");
        checkQuery("SELECT DESCR FROM Tx21 WHERE descr NOT LIKE \"t#####\" ORDER BY ID",
                new Object[][] { { "t1111C" }, { "t1111" } });

        st.close();

        dropTable(tableName);
    }
}
