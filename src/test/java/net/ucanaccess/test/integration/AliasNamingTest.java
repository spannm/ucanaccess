package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.SQLException;

@RunWith(Parameterized.class)
public class AliasNamingTest extends AccessVersionDefaultTest {

    public AliasNamingTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/aliasNaming.accdb";
    }

    @Test
    public void testRegex2() throws SQLException, IOException {
        executeStatements("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
        dumpQueryResult("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`");
    }

}
