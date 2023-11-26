package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;

class MultipleGroupByTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMultiple(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        executeStatements(
            "CREATE TABLE t_xxx (f1 VARCHAR, f2 VARCHAR, f3 VARCHAR, f4 VARCHAR, val NUMBER)",
            "CREATE TABLE t_xxx_ko (f1, f2, val) AS (SELECT f1, f2, SUM(val) FROM t_xxx GROUP BY f1, f2) WITH DATA");
    }
}
