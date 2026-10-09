package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

class MultipleGroupByTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testMultiple(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        executeStatements(
            "CREATE TABLE t_xxx (f1 VARCHAR, f2 VARCHAR, f3 VARCHAR, f4 VARCHAR, val NUMBER)",
            "INSERT INTO t_xxx (f1, f2, f3, f4, val) VALUES ('a', 'x', '1', '1', 1)",
            "INSERT INTO t_xxx (f1, f2, f3, f4, val) VALUES ('a', 'x', '2', '2', 2)",
            "INSERT INTO t_xxx (f1, f2, f3, f4, val) VALUES ('b', 'y', '3', '3', 4)",
            "CREATE TABLE t_xxx_ko (f1, f2, val) AS (SELECT f1, f2, SUM(val) FROM t_xxx GROUP BY f1, f2) WITH DATA");

        checkQuery("SELECT f1, f2, val FROM t_xxx_ko ORDER BY f1", recs(rec("a", "x", 3), rec("b", "y", 4)));
    }
}
