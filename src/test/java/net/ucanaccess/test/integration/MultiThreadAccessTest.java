package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.*;

class MultiThreadAccessTest extends UcanaccessBaseTest {
    private static int intVal;

    private String dbPath;
    private final String tableName = "T1";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        dbPath = getFileAccDb().getAbsolutePath();
        executeStatements("CREATE TABLE " + tableName + " (id COUNTER primary key, descr MEMO)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable(tableName);
    }

    void crud() throws SQLException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        intVal++;
        st.execute("INSERT INTO " + tableName + " (id,descr) VALUES( " + intVal + ",'" + intVal + "Bla bla bla bla:"
                + Thread.currentThread() + "')");
        conn.commit();
        conn.close();
    }

    void crudPS() throws SQLException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " (id,descr) VALUES(?, ?)");
        ps.setInt(1, ++intVal);
        ps.setString(2, "ciao");
        ps.execute();
        ps = conn.prepareStatement("UPDATE " + tableName + " SET descr='" + Thread.currentThread() + "'");
        ps.executeUpdate();
        ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE descr='" + Thread.currentThread() + "'");
        conn.commit();
        conn.close();
    }

    void crudUpdatableRS() throws SQLException {
        Connection conn = getUcanaccessConnection(dbPath);
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        st.execute("INSERT INTO " + tableName + " (id,descr) VALUES(" + (++intVal) + " ,'" + Thread.currentThread() + "')");
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName + "", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.next();
        rs.updateString(2, "" + Thread.currentThread());
        rs.updateRow();
        conn.commit();
        conn.close();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testMultiThread(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        int nt = 50;
        Thread[] threads = new Thread[nt];
        for (int i = 0; i < nt; i++) {
            threads[i] = new Thread(() -> {
                try {
                    crud();
                    crudPS();
                    crudUpdatableRS();
                } catch (SQLException e) {
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
