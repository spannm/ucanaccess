package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class NoRomanCharacterTest extends AccessVersionAllTest {

    public NoRomanCharacterTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Test
    public void testNoRomanCharactersInColumnName() throws Exception {
        dumpQueryResult("SELECT * FROM NOROMAN");

        Statement st = ucanaccess.createStatement();

        st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        st.execute("UPDATE NOROMAN SET  [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
        ResultSet rs = st.executeQuery("SELECT * FROM NOROMAN");
        while (rs.next()) {
            getLogger().info(rs.getString("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß"));
        }

        checkQuery("SELECT * FROM NOROMAN");
        st.close();
    }
}
