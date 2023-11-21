package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;

class CounterTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_counter (cntr COUNTER PRIMARY KEY, chr CHAR(4), descr MEMO)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreateTypes(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

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

}
