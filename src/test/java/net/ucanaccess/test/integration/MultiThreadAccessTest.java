package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.*;

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
        st.execute("INSERT INTO " + tableName + " (id,descr)  VALUES( " + intVal + ",'" + intVal + "Bla bla bla bla:"
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
            threads[i] = new Thread(() -> {
                try {
                    crud();
                    crudPS();
                    crudUpdatableRS();
                } catch (SQLException | IOException e) {
                    e.printStackTrace();
                }
            });
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
