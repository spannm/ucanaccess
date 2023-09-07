package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.ResultSet;
import java.sql.Statement;

class NoRomanCharacterTest extends UcanaccessTestBase {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNoRomanCharactersInColumnName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM NOROMAN");

        Statement st = ucanaccess.createStatement();

        st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]) VALUES( 'the end','yeeep')");
        st.execute("UPDATE NOROMAN SET  [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
        ResultSet rs = st.executeQuery("SELECT * FROM NOROMAN");
        while (rs.next()) {
            getLogger().info(rs.getString("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß"));
        }

        checkQuery("SELECT * FROM NOROMAN");
        st.close();
    }
}
