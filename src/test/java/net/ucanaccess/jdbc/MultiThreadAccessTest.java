package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Try;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MultiThreadAccessTest extends UcanaccessBaseTest {
    private static AtomicInteger intVal = new AtomicInteger(0);

    private String               dbPath;

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        dbPath = getFileAccDb().getAbsolutePath();
        executeStatements("CREATE TABLE t_mta (id COUNTER PRIMARY KEY, descr MEMO)");
    }

    void crud() throws SQLException {
        try (UcanaccessConnection conn = buildConnection().withDbPath(dbPath).build()) {
            conn.setAutoCommit(false);
            UcanaccessStatement st = conn.createStatement();
            intVal.incrementAndGet();
            st.execute("INSERT INTO t_mta (id, descr) VALUES("
                + intVal + ", '" + intVal + " bla bla bla:" + Thread.currentThread() + "')");
            conn.commit();
        }
    }

    void crudPreparedStatement() throws SQLException {
        try (UcanaccessConnection conn = buildConnection().withDbPath(dbPath).build()) {
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("INSERT INTO t_mta (id, descr) VALUES(?, ?)");
            ps.setInt(1, intVal.incrementAndGet());
            ps.setString(2, "ciao");
            ps.execute();
            ps = conn.prepareStatement("UPDATE t_mta SET descr='" + Thread.currentThread() + "'");
            ps.executeUpdate();
            ps = conn.prepareStatement("DELETE FROM t_mta WHERE descr='" + Thread.currentThread() + "'");
            conn.commit();
        }
    }

    void crudUpdatableResultset() throws SQLException {
        try (UcanaccessConnection conn = buildConnection().withDbPath(dbPath).build()) {
            conn.setAutoCommit(false);
            UcanaccessStatement st = conn.createStatement();
            st.execute("INSERT INTO t_mta (id, descr) VALUES(" + intVal.incrementAndGet() + " ,'" + Thread.currentThread() + "')");
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM t_mta", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.updateString(2, "" + Thread.currentThread());
            rs.updateRow();
            conn.commit();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testMultiThread(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        List<Thread> threads = IntStream.range(0, 50).mapToObj(i -> new Thread(() -> assertDoesNotThrow(() -> {
            crud();
            crudPreparedStatement();
            crudUpdatableResultset();
        }))).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread t : threads) {
            Try.catching(() -> t.join()).orIgnore();
        }
        ucanaccess = buildConnection().withDbPath(dbPath).build();
        dumpQueryResult("SELECT * FROM t_mta ORDER BY id");

        checkQuery("SELECT * FROM t_mta ORDER BY id");
    }
}
