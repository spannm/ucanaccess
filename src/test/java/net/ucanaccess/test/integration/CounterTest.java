package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class CounterTest extends UcanaccessBaseTest {
    private String tableName = "t_bbb";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE " + tableName + " (z COUNTER PRIMARY KEY, B CHAR(4), c BLOB, d TEXT)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable(tableName);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreateTypes(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("DISABLE AUTOINCREMENT ON " + tableName);
            // insert arbitrary AutoNumber value
            st.execute("INSERT INTO " + tableName + " (z, b, c, d) VALUES (3, 'C', NULL, NULL)");

            st.execute("ENABLE AUTOINCREMENT ON " + tableName);

            // 4 (verify AutoNumber seed updated)
            st.execute("INSERT INTO " + tableName + " (b, c, d) VALUES ('D', NULL, NULL)");

            st.execute("INSERT INTO " + tableName + " (b, c, d) VALUES ('E', NULL, NULL)"); // 5
            st.execute("INSERT INTO " + tableName + " (b, c, d) VALUES ('F', NULL, NULL)"); // 6

            st.execute("DISABLE AUTOINCREMENT ON " + tableName);
            st.execute("INSERT INTO " + tableName + " (z, b, c, d) VALUES (8, 'H', NULL, NULL)"); // arbitrary, new seed = 9
            st.execute("INSERT INTO " + tableName + " (z, b, c, d) VALUES (7, 'G', NULL, NULL)"); // arbitrary smaller than current seed
            st.execute("INSERT INTO " + tableName + " (z, b, c, d) VALUES (-1, 'A', NULL, NULL)"); // arbitrary negative value
            st.execute("ENABLE AUTOINCREMENT ON " + tableName);
            st.execute("INSERT INTO " + tableName + " (b, c, d) VALUES ('I', NULL, NULL)"); // 9
        }

        dumpQueryResult("SELECT * FROM " + tableName);
        Object[][] ver =
            {{-1, "A", null, null}, {3, "C", null, null}, {4, "D", null, null},
            {5, "E", null, null}, {6, "F", null, null}, {7, "G", null, null},
            {8, "H", null, null}, {9, "I", null, null}};
        checkQuery("SELECT * FROM " + tableName + " ORDER BY z", ver);

    }

}
