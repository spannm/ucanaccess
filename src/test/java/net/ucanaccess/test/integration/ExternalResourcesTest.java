package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

class ExternalResourcesTest extends UcanaccessTestBase {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testLinks(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        File main = copyResourceToTempFile(TEST_DB_DIR + "main.mdb");
        File linkee1 = copyResourceToTempFile(TEST_DB_DIR + "linkee1.mdb");
        File linkee2 = copyResourceToTempFile(TEST_DB_DIR + "linkee2.mdb");
        String url = UcanaccessDriver.URL_PREFIX + main.getAbsolutePath() + ";immediatelyreleaseresources=true"
                + ";remap=c:\\db\\linkee1.mdb|" + linkee1.getAbsolutePath() + "&c:\\db\\linkee2.mdb|"
                + linkee2.getAbsolutePath();
        getLogger().info("Database url: {}", url);
        Connection conn = DriverManager.getConnection(url, "", "");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT * FROM table1");
        dumpQueryResult(rs);
        rs = st.executeQuery("SELECT * FROM table2");
        dumpQueryResult(rs);
        rs.close();
        st.close();
        conn.close();
    }
}
