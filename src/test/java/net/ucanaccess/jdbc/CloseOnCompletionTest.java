package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class CloseOnCompletionTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCloseOnCompletion(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (PreparedStatement st = ucanaccess.prepareStatement("CREATE TABLE t_cloc (id VARCHAR(23))")) {
            st.closeOnCompletion();
            st.execute();
        }
    }

}
