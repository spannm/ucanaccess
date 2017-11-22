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
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class CrudTest extends AccessVersionAllTest {

    public CrudTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE T1 (id LONG,descr TEXT) ");
    }

    @Test
    public void testCrud() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        int id = 6666554;
        int id1 = 5556664;

        st.execute("DELETE FROM t1");
        st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id + ",'nel mezzo del cammin di nostra vita')");
        boolean ret = getCount("SELECT COUNT(*) FROM T1") == 1;
        assertTrue("Failed Insert", ret);
        st.executeUpdate("UPDATE T1 SET id=" + id1 + " WHERE  id=" + id);
        ret = getCount("SELECT COUNT(*) FROM T1 where id=" + id1) == 1;
        assertTrue("Failed Update", ret);
        st.executeUpdate("DELETE FROM  T1  WHERE  id=" + id1);
        ret = getCount("SELECT COUNT(*) FROM T1 where id=" + id1) == 0;
        assertTrue("Failed Delete", ret);
        st.close();
    }

    @Test
    public void testCrudPS() throws SQLException, IOException {
        PreparedStatement ps = null;
        Statement st = null;
        st = ucanaccess.createStatement();

        st.execute("delete from t1");
        ps = ucanaccess.prepareStatement("INSERT INTO T1 (id,descr)  VALUES( ?,?)");
        int id = 6666554;
        int id1 = 5556664;
        ps.setInt(1, id);
        ps.setString(2, "Prep1");
        ps.execute();

        boolean ret = getCount("SELECT COUNT(*) FROM T1") == 1;
        assertTrue("Failed Insert", ret);
        ps.close();
        ps = ucanaccess.prepareStatement("UPDATE T1 SET id=? WHERE  id=?");
        ps.setInt(1, id1);
        ps.setInt(2, id);
        ps.executeUpdate();
        ret = getCount("SELECT COUNT(*) FROM T1 where id=" + id1) == 1;
        assertTrue("Failed Update", ret);
        ps.close();
        ps = ucanaccess.prepareStatement("DELETE * FROM  t1  WHERE  id=?");
        ps.setInt(1, id1);
        ps.executeUpdate();
        ret = getCount("SELECT COUNT(*) FROM T1 where id=" + id1) == 0;
        assertTrue("Failed Delete", ret);
        ps.close();
        st.close();
    }

    @Test
    public void testCrudPSBatch() throws SQLException, IOException {
        PreparedStatement ps = null;
        try {

            ps = ucanaccess.prepareStatement("INSERT INTO T1 (id,descr)  VALUES( ?,?)");
            int id = 1234;
            int id1 = 12345;
            ps.setInt(1, id);
            ps.setString(2, "Prep1");
            ps.addBatch();
            ps.setInt(1, id1);
            ps.setString(2, "Prep2");
            ps.addBatch();
            ps.executeBatch();
            Object[][] ver = { { 1234, "Prep1" }, { 12345, "Prep2" } };
            checkQuery("SELECT *  FROM T1", ver);
            boolean ret = getCount("SELECT COUNT(*) FROM T1 where id in (1234,12345)") == 2;
            ps.clearBatch();
            assertTrue("Failed Insert", ret);
            ps.close();
            ps = ucanaccess.prepareStatement("DELETE FROM  t1 ");
            ps.addBatch();
            ps.executeBatch();
            ret = getCount("SELECT COUNT(*) FROM T1 ") == 0;
            assertTrue("Failed Delete", ret);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Test
    public void testUpdatableRS() throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            st = ucanaccess.createStatement();
            int id = 6666554;
            st.execute("delete from t1");
            st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id + ",'tre canarini volano su e cadono')");
            PreparedStatement ps = ucanaccess.prepareStatement("SELECT *  FROM T1", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            rs = ps.executeQuery();
            rs.next();
            rs.updateString(2, "show must go off");
            rs.updateRow();
            Object[][] ver = { { 6666554, "show must go off" } };
            checkQuery("SELECT *  FROM T1", ver);
            st.execute("delete from t1");
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }

        }
    }

    @Test
    public void testDeleteRS() throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            ucanaccess.setAutoCommit(false);
            st = ucanaccess.createStatement();
            int id = 6666554;
            st.execute("delete from t1");
            st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id + ",'tre canarini volano su e cadono')");
            PreparedStatement ps = ucanaccess.prepareStatement("SELECT *  FROM T1", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            rs = ps.executeQuery();
            rs.next();

            rs.deleteRow();
            ps.getConnection().commit();

            checkQuery("SELECT COUNT(*) FROM T1 ", 0);

        } finally {
            if (rs != null) {
                rs.close();
            }
        }

    }

    @Test
    public void testInsertRS() throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            ucanaccess.setAutoCommit(false);
            st = ucanaccess.createStatement();
            int id = 6666554;
            st.execute("delete from t1");
            st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id + ",'tre canarini volano su e cadono')");
            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM T1", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            rs = ps.executeQuery();
            rs.moveToInsertRow();
            rs.updateInt(1, 4);
            rs.updateString(2, "Growing old in rural pleaces");

            rs.insertRow();
            ps.getConnection().commit();
            Object[][] ver = { { 4, "Growing old in rural pleaces" }, { 6666554, "tre canarini volano su e cadono" } };
            checkQuery("SELECT * FROM T1 order by id", ver);
            st.execute("delete from t1");
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }

        }
    }

    @Test
    public void testInsertRSNoAllSet() throws SQLException, IOException {

        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        st.execute(" CREATE TABLE T2 (id AUTOINCREMENT,descr TEXT) ");
        st.execute("delete from t2");

        PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM T2", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.moveToInsertRow();
        rs.updateInt(1, 0);
        rs.updateString(2, "Growing old in rural places");

        rs.insertRow();
        rs = ps.getGeneratedKeys();
        rs.next();
        ps.getConnection().commit();

        checkQuery("SELECT * FROM T2 order by id", 1, "Growing old in rural places");
        Statement stat = ucanaccess.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs1 = stat.executeQuery("SELECT * FROM T2 order by id");
        rs1.last();

        st.execute("delete from t1");

        rs.close();
        st.close();
    }

    @Test
    public void testPartialInsertRS() throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            ucanaccess.setAutoCommit(false);
            st = ucanaccess.createStatement();
            st.execute("CREATE TABLE T21 (id autoincrement,descr TEXT) ");

            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM T21", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            rs = ps.executeQuery();
            rs.moveToInsertRow();

            rs.updateString(2, "Growing old without emotions");

            rs.insertRow();
            ps.getConnection().commit();
            Object[][] ver = { { 1, "Growing old without emotions" } };
            checkQuery("SELECT * FROM T21 order by id", ver);
            st.execute("delete from t21");
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }

        }
    }

}
