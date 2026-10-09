package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.File;
import java.lang.System.Logger.Level;
import java.sql.SQLException;
import java.sql.SQLWarning;

class FolderTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testFolderContent(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        String folderPath = System.getProperty("accessFolder");
        assumeTrue(folderPath != null, "System property accessFolder not set");

        File[] files = new File(folderPath).listFiles();
        assertThat(files).as("Content of folder %s", folderPath).isNotNull();

        for (File fl : files) {
            try (UcanaccessConnection conn = buildConnection()
                .withDbPath(fl.getAbsolutePath())
                .build()) {
                assertThat(conn.isClosed()).isFalse();
                getLogger().log(Level.INFO, "open {0}", fl.getAbsolutePath());
                SQLWarning sqlw = conn.getWarnings();
                while (sqlw != null) {
                    getLogger().log(Level.INFO, sqlw.getMessage());
                    sqlw = sqlw.getNextWarning();
                }
            }
        }
    }
}
