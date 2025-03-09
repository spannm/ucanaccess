package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.List;

class UnproperExecuteQueryTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "noRoman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testExecute(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            String tableName = "t_noroman";
            for (String sql : List.of(
                "INSERT INTO " + tableName + " ([end], [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]) VALUES('the end', 'yeeep')",
                "UPDATE " + tableName + " SET [ENd] = 'BLeah'",
                "DELETE FROM " + tableName)) {
                    assertThatThrownBy(() -> st.executeQuery(sql))
                        .isInstanceOf(UcanaccessSQLException.class)
                        .hasMessageMatching("UCAExc:::[0-9]\\.[0-9][0-9\\.]*(?:-SNAPSHOT)? General error");
                    st.execute(sql);
            }
        }
    }

}
