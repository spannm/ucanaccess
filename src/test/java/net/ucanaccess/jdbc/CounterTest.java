package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.sql.SQLException;

class CounterTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);
        executeStatements("CREATE TABLE t_counter (cntr COUNTER PRIMARY KEY, chr CHAR(4), descr MEMO)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCreateTypes(AccessVersion accessVersion) throws SQLException, IOException {
        init(accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            String table = "t_counter";
            executeStatements(st, "DISABLE AUTOINCREMENT ON " + table);
            assertTrue(st.getConnection().getDbIO().getTable(table).isAllowAutoNumberInsert());

            executeStatements(st,
                "INSERT INTO t_counter (cntr, chr, descr) VALUES (3, 'C', 'autoincr OFF, insert arbitrary AutoNumber value')");

            executeStatements(st,
                "ENABLE AUTOINCREMENT ON t_counter");
            assertFalse(st.getConnection().getDbIO().getTable(table).isAllowAutoNumberInsert());

            executeStatements(st,
                "INSERT INTO t_counter (chr, descr) VALUES ('D', 'autoincr ON, expecting cntr=4 (verify AutoNumber seed updated)')",
                "INSERT INTO t_counter (chr, descr) VALUES ('E', 'autoincr ON, expecting cntr=5')",
                "INSERT INTO t_counter (chr, descr) VALUES ('F', 'autoincr ON, expecting cntr=6')",

                "DISABLE AUTOINCREMENT ON t_counter",
                "INSERT INTO t_counter (cntr, chr, descr) VALUES (8, 'H', 'autoincr OFF, arbitrary, new seed = 9')", //
                "INSERT INTO t_counter (cntr, chr, descr) VALUES (7, 'G', 'autoincr OFF, arbitrary smaller than current seed')",
                "INSERT INTO t_counter (cntr, chr, descr) VALUES (-1, 'A', 'autoincr OFF, arbitrary negative value')",
                "ENABLE AUTOINCREMENT ON t_counter",
                "INSERT INTO t_counter (chr, descr) VALUES ('I', 'autoincr ON')"); // cntr=9
        }

        dumpQueryResult("SELECT * FROM t_counter ORDER BY cntr");

        checkQuery("SELECT cntr, chr FROM t_counter ORDER BY cntr", recs(
            rec(-1, "A"),
            rec(3, "C"),
            rec(4, "D"),
            rec(5, "E"),
            rec(6, "F"),
            rec(7, "G"),
            rec(8, "H"),
            rec(9, "I")
        ));

    }

    /**
     * Regression test for a bug where Jackcess's internal table cache holds {@link io.github.spannm.jackcess.Table}
     * instances only via {@code WeakReference}. Without pinning, a GC pass between the {@code DISABLE AUTOINCREMENT
     * ON} DDL statement and a subsequent {@code INSERT} could collect the {@code Table} object whose
     * {@code allowAutoNumberInsert} flag had just been toggled; the table would then be silently reloaded with the
     * flag reset to its default, and an explicit AutoNumber value would be replaced by an auto-generated one.
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2016")
    void testDisableAutoincrementSurvivesGc(AccessVersion accessVersion) throws SQLException, IOException {
        init(accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            executeStatements(st, "DISABLE AUTOINCREMENT ON t_counter");
            assertTrue(st.getConnection().getDbIO().getTable("t_counter").isAllowAutoNumberInsert());

            for (int i = 0; i < 5; i++) {
                System.gc();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            assertTrue(st.getConnection().getDbIO().getTable("t_counter").isAllowAutoNumberInsert(),
                "allowAutoNumberInsert must survive a GC pass between DDL and DML");

            executeStatements(st,
                "INSERT INTO t_counter (cntr, chr, descr) VALUES (3, 'C', 'autoincr OFF, insert arbitrary AutoNumber value')");
        }

        checkQuery("SELECT cntr, chr FROM t_counter ORDER BY cntr", recs(rec(3, "C")));
    }

}
