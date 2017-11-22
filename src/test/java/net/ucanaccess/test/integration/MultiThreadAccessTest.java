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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class MultiThreadAccessTest extends AccessVersionDefaultTest {
    private static int intVal;

    private String       dbPath;
    private final String tableName = "T1";

    public MultiThreadAccessTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        dbPath = getFileAccDb().getAbsolutePath();
        executeStatements("CREATE TABLE " + tableName + " (id COUNTER primary key, descr MEMO)");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable(tableName);
    }

    public void crud() throws SQLException, IOException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        ++intVal;
        st.execute("INSERT INTO " + tableName + " (id,descr)  VALUES( " + (intVal) + ",'" + (intVal) + "Bla bla bla bla:"
                + Thread.currentThread() + "')");
        conn.commit();
        conn.close();
    }

    public void crudPS() throws SQLException, IOException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " (id,descr)  VALUES(?, ?)");
        ps.setInt(1, ++intVal);
        ps.setString(2, "ciao");
        ps.execute();
        ps = conn.prepareStatement("UPDATE " + tableName + " SET descr='" + Thread.currentThread() + "'");
        ps.executeUpdate();
        ps = conn.prepareStatement("DELETE FROM  " + tableName + "  WHERE  descr='" + Thread.currentThread() + "'");
        conn.commit();
        conn.close();
    }

    public void crudUpdatableRS() throws SQLException, IOException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        st.execute("INSERT INTO " + tableName + " (id,descr)  VALUES(" + (++intVal) + "  ,'" + Thread.currentThread() + "')");
        PreparedStatement ps = conn.prepareStatement("SELECT *  FROM " + tableName + "", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.next();
        rs.updateString(2, "" + Thread.currentThread());
        rs.updateRow();
        conn.commit();
        conn.close();
    }

    @Test
    public void testMultiThread() throws SQLException, IOException {
        int nt = 50;
        Thread[] threads = new Thread[nt];
        for (int i = 0; i < nt; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        crud();
                        crudPS();
                        crudUpdatableRS();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            threads[i].start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ucanaccess = getUcanaccessConnection(dbPath);
        dumpQueryResult("SELECT * FROM " + tableName + " ORDER BY id");

        checkQuery("SELECT * FROM " + tableName + " ORDER BY id");
    }
}
