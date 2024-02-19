package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.System.Logger.Level;
import java.sql.ResultSet;

class NoRomanCharacterTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "noroman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNoRomanCharactersInColumnName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM t_noroman");

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_noroman ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]) VALUES( 'the end','yeeep')");
            st.execute("UPDATE t_noroman SET [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
            ResultSet rs = st.executeQuery("SELECT * FROM t_noroman");
            while (rs.next()) {
                getLogger().log(Level.DEBUG, rs.getString("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß"));
            }
        }
        checkQuery("SELECT * FROM t_noroman");
    }
}
