package net.ucanaccess.test.integration;

import java.io.File;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class CreateDatabaseTest extends AccessVersionAllTest {

    public CreateDatabaseTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testNewDatabase() throws Exception {
        // create file name
        File fileMdb = File.createTempFile("CreateDatabaseTest", fileFormat.getFileExtension(), TEST_DB_TEMP_DIR);
        fileMdb.delete();

        String url = UcanaccessDriver.URL_PREFIX + fileMdb.getAbsolutePath() + ";immediatelyReleaseResources=true;newDatabaseVersion=" + fileFormat.name();
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        UcanaccessConnection ucanaccessConnection = (UcanaccessConnection) DriverManager.getConnection(url, "", "");
        assertNotNull(ucanaccessConnection);
        ucanaccess.close();
        ucanaccess = ucanaccessConnection;

        getLogger().info("Database file successfully created: {}", fileMdb.getAbsolutePath());
        Statement st = ucanaccessConnection.createStatement();
        st.execute("CREATE TABLE AAA ( baaaa text(3) PRIMARY KEY,A long default 3, C text(4) ) ");
        st.close();
        st = ucanaccessConnection.createStatement();
        st.execute("INSERT INTO AAA(baaaa,c) VALUES ('33A','G'   )");
        st.execute("INSERT INTO AAA VALUES ('33B',111,'G'   )");
        dumpQueryResult("SELECT * FROM AAA");
    }
}
