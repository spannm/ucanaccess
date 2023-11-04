package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;

class FolderTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testFolderContent(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);

        String folderPath = System.getProperty("accessFolder");
        if (folderPath == null) {
            return;
        }

        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        File folder = new File(folderPath);
        for (File fl : folder.listFiles()) {
            String url = UcanaccessDriver.URL_PREFIX + fl.getAbsolutePath();
            Connection conn = DriverManager.getConnection(url);
            SQLWarning sqlw = conn.getWarnings();
            getLogger().info("open {}", fl.getAbsolutePath());
            while (sqlw != null) {
                getLogger().info(sqlw.getMessage());
                sqlw = sqlw.getNextWarning();
            }
        }
    }
}
