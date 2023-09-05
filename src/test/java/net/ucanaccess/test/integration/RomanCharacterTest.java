package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Statement;

@RunWith(Parameterized.class)
public class RomanCharacterTest extends AccessVersionDefaultTest {

    public RomanCharacterTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Test
    public void testNoRomanCharactersInColumnName() throws Exception {
        dumpQueryResult("SELECT * FROM NOROMAN");
        getLogger().info("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß");
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
            st.execute("UPDATE NOROMAN SET [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
            checkQuery("SELECT * FROM NOROMAN");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

}
