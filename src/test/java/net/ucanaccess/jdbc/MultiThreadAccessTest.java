package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Try;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MultiThreadAccessTest extends UcanaccessBaseTest {
    private static int   intVal;

    private String       dbPath;
    private final String tableName = "T1";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        dbPath = getFileAccDb().getAbsolutePath();
        executeStatements("CREATE TABLE " + tableName + " (id COUNTER primary key, descr MEMO)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE " + tableName);
    }

    void crud() throws SQLException {
        try (Connection conn = buildConnection().withDbPath(dbPath).build()) {
            conn.setAutoCommit(false);
            Statement st = conn.createStatement();
            intVal++;
            st.execute("INSERT INTO " + tableName + " (id,descr) VALUES( " + intVal + ",'" + intVal + "Bla bla bla bla:"
                    + Thread.currentThread() + "')");
            conn.commit();
        }
    }

    void crudPS() throws SQLException {
        try (Connection conn = buildConnection().withDbPath(dbPath).build()) {
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " (id,descr) VALUES(?, ?)");
            ps.setInt(1, ++intVal);
            ps.setString(2, "ciao");
            ps.execute();
            ps = conn.prepareStatement("UPDATE " + tableName + " SET descr='" + Thread.currentThread() + "'");
            ps.executeUpdate();
            ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE descr='" + Thread.currentThread() + "'");
            conn.commit();
        }
    }

    void crudUpdatableRS() throws SQLException {
        try (Connection conn = buildConnection().withDbPath(dbPath).build()) {
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
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testMultiThread(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        List<Thread> threads = IntStream.range(0, 50).mapToObj(i -> new Thread(() -> {
            assertDoesNotThrow(() -> {
                crud();
                crudPS();
                crudUpdatableRS();
            });
        })).collect(Collectors.toList());
        
        threads.forEach(Thread::start);

        for (Thread t : threads) {
            Try.catching(() -> t.join()).orIgnore();
        }
        ucanaccess = buildConnection().withDbPath(dbPath).build();
        dumpQueryResult("SELECT * FROM " + tableName + " ORDER BY id");

        checkQuery("SELECT * FROM " + tableName + " ORDER BY id");
    }
}
