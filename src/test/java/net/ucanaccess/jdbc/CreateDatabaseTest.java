package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.sql.DriverManager;
import java.sql.Statement;

class CreateDatabaseTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNewDatabase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        File fileMdb = createTempFileName(getClass().getSimpleName());
        fileMdb.deleteOnExit();

        String url = UcanaccessDriver.URL_PREFIX + fileMdb.getAbsolutePath() + ";immediatelyReleaseResources=true;newDatabaseVersion=" + getFileFormat().name();
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        UcanaccessConnection ucanaccessConnection = (UcanaccessConnection) DriverManager.getConnection(url, "", "");
        assertNotNull(ucanaccessConnection);
        ucanaccess.close();
        ucanaccess = ucanaccessConnection;

        getLogger().info("Database file successfully created: {}", fileMdb.getAbsolutePath());

        try (Statement st = ucanaccessConnection.createStatement()) {
            st.execute("CREATE TABLE AAA (baaaa text(3) PRIMARY KEY, A long default 3, C text(4))");
            st.execute("INSERT INTO AAA(baaaa, c) VALUES ('33A','G' )");
            st.execute("INSERT INTO AAA VALUES ('33B',111,'G' )");
        }
        dumpQueryResult("SELECT * FROM AAA");
    }

}
