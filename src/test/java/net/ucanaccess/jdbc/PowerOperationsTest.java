package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

/**
 * Unit test for SQL power operations, including both the Access-specific
 * '^' operator and the standard SQL POWER() function to verify that
 * UCanAccess correctly handles power calculations in various SQL contexts.
 */
class PowerOperationsTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);

        executeStatements(
            "CREATE TABLE t_powerop (id COUNTER, num NUMERIC(12,3), exp_val NUMERIC(12,3))",
            "INSERT INTO t_powerop (num, exp_val) VALUES(-2.0, 2.0)",
            "INSERT INTO t_powerop (num, exp_val) VALUES(4.5, 2.0)",
            "INSERT INTO t_powerop (num, exp_val) VALUES(8.0, 1.0000/3.0000)", // base 8, exponent 1/3 (cube root)
            "INSERT INTO t_powerop (num, exp_val) VALUES(10.0, -1.0)"          // base 10, exponent -1
        );

        dumpQueryResult("SELECT * FROM t_powerop");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testPowerOperatorTranslation(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // basic num^2 check
        checkQuery("SELECT num^2 FROM t_powerop ORDER BY num", recs(rec(4), rec(20.25), rec(64), rec(100)));

        // num > 2^2 (simple constant exponent)
        checkQuery("SELECT num FROM t_powerop WHERE num > 2^2", recs(rec(4.5), rec(8), rec(10)));

        checkQuery("SELECT num^exp_val FROM t_powerop ORDER BY num", recs(
            rec(-2.0 * -2.0), // -2.0 ^ 2.0 = 4.0
            rec(4.5 * 4.5),   // 4.5 ^ 2.0 = 20.25
            rec(2.0),         // 8.0 ^ (1.0/3.0) = 2.0 (cube root of 8)
            rec(0.1)          // 10.0 ^ -1.0 = 0.1
        ));

        // mixed constant and column in expression
        checkQuery("SELECT (num + 1)^2 FROM t_powerop WHERE num = -2.0", recs(rec(1))); // (-2.0 + 1)^2 = (-1)^2 = 1
        checkQuery("SELECT (num * 2)^exp_val FROM t_powerop WHERE num = 4.5", recs(rec(81))); // (4.5 * 2)^2.0 = 9.0^2.0 = 81.0

        // in WHERE clause with column as exponent
        checkQuery("SELECT num FROM t_powerop WHERE num > 2^exp_val AND num < 10", recs(rec(4.5), rec(8))); // num > 2^2.0 (4) -> 4.5, 8.0; AND num < 10 -> 4.5, 8.0
        checkQuery("SELECT num FROM t_powerop WHERE num > 2^exp_val AND num < 10 AND num <> 8.0", recs(rec(4.5)));

        // negative base with odd/even exponent (Access handles this differently than standard Math.pow sometimes)
        // Access VBA ^ operator: (-2)^2 = 4; (-8)^(1/3) = -2 (Math.pow gives NaN for negative base and non-integer exponent)
        checkQuery("SELECT num FROM t_powerop WHERE num = -2.0 AND (num^exp_val) = 4.0", recs(rec(-2)));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testPowerFunction(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // basic POWER(num, 2) check
        checkQuery("SELECT num, POWER(num, 2) FROM t_powerop ORDER BY num", recs(
            rec(-2.0, 4),
            rec(4.5, 20.25),
            rec(8.0, 64),
            rec(10.0, 100)
        ));

        // num > POWER(2, 2) (simple constant exponent)
        checkQuery("SELECT num FROM t_powerop WHERE num > POWER(2, 2)", recs(rec(4.5), rec(8), rec(10)));

        // column as exponent (POWER(num, exp_val))
        checkQuery("SELECT num, exp_val, POWER(num, exp_val) FROM t_powerop ORDER BY num", recs(
            rec(-2.0, 2.0, 4),      // POWER(-2.0, 2.0) = 4.0
            rec(4.5, 2.0, 20.25),   // POWER(4.5, 2.0) = 20.25
            rec(8.0, 1.0/3.0, 2.0), // POWER(8.0, 1.0/3.0) = 2.0
            rec(10.0, -1.0, 0.1)    // POWER(10.0, -1.0) = 0.1
        ));

        // mixed constant and column in POWER function
        checkQuery("SELECT POWER(num + 1, 2) FROM t_powerop WHERE num = -2.0", recs(rec(1))); // POWER(-2.0 + 1, 2) = POWER(-1, 2) = 1
        checkQuery("SELECT POWER(num * 2, exp_val) FROM t_powerop WHERE num = 4.5", recs(rec(81))); // POWER(4.5 * 2, 2.0) = POWER(9.0, 2.0) = 81.0
   }
}
