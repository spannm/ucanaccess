package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Statement;

class RomanCharacterTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testNoRomanCharactersInColumnName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM t_noroman");
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_noroman ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]) VALUES('the end', 'yeeep')");
            st.execute("UPDATE t_noroman SET [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
        }
        checkQuery("SELECT * FROM t_noroman");
    }

}
