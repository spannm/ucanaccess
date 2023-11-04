package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class CloseOnCompletionTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCloseOnCompletion(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (PreparedStatement st = ucanaccess.prepareStatement("CREATE TABLE pluto1 (id varchar(23))")) {
            st.closeOnCompletion();
            st.execute();
        }
    }

}
