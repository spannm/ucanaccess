package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.sql.Statement;

class UnproperExecuteQueryTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testExecute(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        execute("UPDATE NOROMAN SET [ENd]='BLeah'");
        execute("DELETE FROM NOROMAN");
    }

    private void execute(String s) throws SQLException {
        Statement st = ucanaccess.createStatement();
        try {
            st.executeQuery(s);
            fail("Should not get here");
        } catch (Exception _ex) {
            getLogger().info(_ex.getMessage());
        }
        st.execute(s);
    }
}
