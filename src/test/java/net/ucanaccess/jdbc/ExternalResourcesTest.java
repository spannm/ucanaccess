package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.*;

class ExternalResourcesTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testLinks(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);

        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        File main = copyResourceToTempFile(TEST_DB_DIR + "main.mdb");
        File linkee1 = copyResourceToTempFile(TEST_DB_DIR + "linkee1.mdb");
        File linkee2 = copyResourceToTempFile(TEST_DB_DIR + "linkee2.mdb");
        String url = UcanaccessDriver.URL_PREFIX + main.getAbsolutePath() + ";immediatelyreleaseresources=true"
                + ";remap=c:\\db\\linkee1.mdb|" + linkee1.getAbsolutePath() + "&c:\\db\\linkee2.mdb|"
                + linkee2.getAbsolutePath();
        getLogger().info("Database url: {}", url);

        try (Connection conn = DriverManager.getConnection(url, "", "");
            Statement st = conn.createStatement()) {

            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table1"));
            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table2"));
        }
    }

}
