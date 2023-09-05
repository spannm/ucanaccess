package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.sql.*;

@RunWith(Parameterized.class)
public class FolderTest extends AccessVersionDefaultTest {

    public FolderTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testFolderContent() throws SQLException, ClassNotFoundException {
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
                getLogger().info("error {}", fl.getAbsolutePath());
            } finally {
                if (st != null) {
                    st.close();
                }
            }
        }
    }
}
