package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;

class AliasNamingTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "aliasNaming.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testRegex2(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        executeStatements("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
        dumpQueryResult("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
    }

}
