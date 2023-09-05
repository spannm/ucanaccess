package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class UnproperExecuteQueryTest extends AccessVersionAllTest {

    public UnproperExecuteQueryTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Test
    public void testExecute() throws Exception {
        execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        execute("UPDATE NOROMAN SET [ENd]='BLeah'");
        execute("delete from NOROMAN");
    }

    private void execute(String s) throws SQLException {
        Statement st = ucanaccess.createStatement();
        try {
            st.executeQuery(s);
            fail("Should not get here");
        } catch (Exception e) {
            // e.printStackTrace();
            getLogger().info(e.getMessage());
        }
        st.execute(s);
    }
}
