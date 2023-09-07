package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.*;

class FolderTest extends UcanaccessTestBase {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testFolderContent(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);
        Statement st = null;
        String folderPath = System.getProperty("accessFolder");
        if (folderPath == null) {
            return;
        }
        File folder = new File(folderPath);
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        for (File fl : folder.listFiles()) {
            try {
                String url = UcanaccessDriver.URL_PREFIX + fl.getAbsolutePath();
                Connection conn = DriverManager.getConnection(url);
                SQLWarning sqlw = conn.getWarnings();
                getLogger().info("open {}", fl.getAbsolutePath());
                while (sqlw != null) {
                    getLogger().info(sqlw.getMessage());
                    sqlw = sqlw.getNextWarning();
                }

            } catch (Exception _ex) {
                getLogger().warn("error {}: ", fl.getAbsolutePath(), _ex);
            } finally {
                if (st != null) {
                    st.close();
                }
            }
        }
    }
}
