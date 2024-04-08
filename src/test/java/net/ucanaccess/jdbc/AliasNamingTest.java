package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

class AliasNamingTest extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testRegex2(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        executeStatements("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
        dumpQueryResult("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
    }

}
