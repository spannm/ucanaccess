package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class CounterTest extends AccessVersionAllTest {
    private String tableName = "T_BBB";

    public CounterTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE " + tableName + " ( Z COUNTER PRIMARY KEY, B char(4), C blob, d TEXT )");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable(tableName);
    }

    @Test
    public void testCreateTypes() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("DISABLE AUTOINCREMENT ON " + tableName);
        st.execute("INSERT INTO " + tableName + " (Z,B,C,D) VALUES (3,'C',NULL,NULL)"); // insert arbitrary
                                                                                        // AutoNumber value

        st.execute("ENABLE AUTOINCREMENT ON " + tableName);

        st.execute("INSERT INTO " + tableName + " (B,C,D) VALUES ('D',NULL,NULL)"); // 4 (verify AutoNumber seed
                                                                                    // updated)

        st.execute("INSERT INTO " + tableName + " (B,C,D) VALUES ('E' ,NULL,NULL)"); // 5
        st.execute("INSERT INTO " + tableName + " (B,C,D) VALUES ('F',NULL,NULL)"); // 6

        st.execute("DISABLE AUTOINCREMENT ON " + tableName);
        st.execute("INSERT INTO " + tableName + " (Z,B,C,D) VALUES (8,'H',NULL,NULL)"); // arbitrary, new seed = 9
        st.execute("INSERT INTO " + tableName + " (Z,B,C,D) VALUES (7,'G',NULL,NULL)"); // arbitrary smaller than
                                                                                        // current seed
        st.execute("INSERT INTO " + tableName + " (Z,B,C,D) VALUES (-1,'A',NULL,NULL)"); // arbitrary negative value
        st.execute("ENABLE AUTOINCREMENT ON " + tableName);
        st.execute("INSERT INTO " + tableName + " (B,C,D) VALUES ('I',NULL,NULL)"); // 9
        Object[][] ver =
            {{-1, "A", null, null}, {3, "C", null, null}, {4, "D", null, null}, {5, "E", null, null}, {6, "F", null, null}, {7, "G", null, null}, {8, "H", null, null}, {9, "I", null, null}};
        dumpQueryResult("SELECT * FROM " + tableName);
        checkQuery("SELECT * FROM " + tableName + " order by Z", ver);

        st.close();
    }

}
