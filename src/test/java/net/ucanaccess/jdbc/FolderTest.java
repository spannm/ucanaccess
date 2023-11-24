package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.SQLException;
import java.sql.SQLWarning;

class FolderTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testFolderContent(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);

        String folderPath = System.getProperty("accessFolder");
        if (folderPath == null) {
            return;
        }

        File folder = new File(folderPath);
        for (File fl : folder.listFiles()) {
            UcanaccessConnection conn = buildConnection()
                .withDbPath(fl.getAbsolutePath())
                .build();
            SQLWarning sqlw = conn.getWarnings();
            getLogger().info("open {}", fl.getAbsolutePath());
            while (sqlw != null) {
                getLogger().info(sqlw.getMessage());
                sqlw = sqlw.getNextWarning();
            }
        }
    }
}
