package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class AliasNamingTest extends UcanaccessBaseFileTest {

    @Test
    void testRegex2() throws SQLException {
        init();
        executeStatements("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
        dumpQueryResult("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
    }

}
