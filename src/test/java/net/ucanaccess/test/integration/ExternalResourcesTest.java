package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class ExternalResourcesTest extends AccessVersionDefaultTest {

    public ExternalResourcesTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testLinks() throws Exception {
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

        File main = copyResourceToTempFile("testdbs/main.mdb");
        File linkee1 = copyResourceToTempFile("testdbs/linkee1.mdb");
        File linkee2 = copyResourceToTempFile("testdbs/linkee2.mdb");
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
