package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2016Test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class BigintTest extends AccessVersion2016Test {

    public BigintTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testBigintInsert() throws Exception {
        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE table1 (entry TEXT(50) PRIMARY KEY, x BIGINT)");
        Long expected = 3000000000L;
        String sql = String.format("INSERT INTO table1 (entry, x) VALUES ('3 billion', %d)", expected);
        st.execute(sql);
        st.close();
        ucanaccess.close();
        String connUrl = UcanaccessDriver.URL_PREFIX + accdbPath + ";immediatelyReleaseResources=true";
        Connection cnxn = DriverManager.getConnection(connUrl);
        st = cnxn.createStatement();
        ResultSet rs = st.executeQuery("SELECT x FROM table1 WHERE entry='3 billion'");
        rs.next();
        Long actual = rs.getLong("x");
        assertEquals(expected, actual);
        cnxn.close();
    }
}
