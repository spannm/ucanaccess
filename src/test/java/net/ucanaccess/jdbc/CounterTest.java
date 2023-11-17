package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.sql.Statement;

class CounterTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_counter (cntr COUNTER PRIMARY KEY, chr CHAR(4), blb BLOB, txt TEXT)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE t_counter");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreateTypes(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            executeStatements(st,
                "DISABLE AUTOINCREMENT ON t_counter",
                // insert arbitrary AutoNumber value
                "INSERT INTO t_counter (cntr, chr, blb, txt) VALUES (3, 'C', NULL, NULL)",

                "ENABLE AUTOINCREMENT ON t_counter",

                // expecting cntr=4 (verify AutoNumber seed updated)
                "INSERT INTO t_counter (chr, blb, txt) VALUES ('D', NULL, NULL)",

                "INSERT INTO t_counter (chr, blb, txt) VALUES ('E', NULL, NULL)", // cntr=5
                "INSERT INTO t_counter (chr, blb, txt) VALUES ('F', NULL, NULL)", // cntr=6

                "DISABLE AUTOINCREMENT ON t_counter",
                "INSERT INTO t_counter (cntr, chr, blb, txt) VALUES (8, 'H', NULL, NULL)", // arbitrary, new seed = 9
                "INSERT INTO t_counter (cntr, chr, blb, txt) VALUES (7, 'G', NULL, NULL)", // arbitrary smaller than current seed
                "INSERT INTO t_counter (cntr, chr, blb, txt) VALUES (-1, 'A', NULL, NULL)", // arbitrary negative value
                "ENABLE AUTOINCREMENT ON t_counter",
                "INSERT INTO t_counter (chr, blb, txt) VALUES ('I', NULL, NULL)"); // cntr=9
        }

        dumpQueryResult("SELECT * FROM t_counter");
        checkQuery("SELECT * FROM t_counter ORDER BY cntr", recs(
            rec(-1, "A", null, null),
            rec(3, "C", null, null),
            rec(4, "D", null, null),
            rec(5, "E", null, null),
            rec(6, "F", null, null),
            rec(7, "G", null, null),
            rec(8, "H", null, null),
            rec(9, "I", null, null)
        ));

    }

}
