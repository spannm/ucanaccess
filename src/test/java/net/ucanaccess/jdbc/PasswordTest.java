package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;

class PasswordTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "pwd.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testPassword(AccessVersion _accessVersion) throws Exception {

        assertThatThrownBy(() -> init(_accessVersion))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Password authentication failed");

        setPassword("ucanaccess");
        try (Connection ucanaccessConnection = createUcanaccessConnection()) {
            assertNotNull(ucanaccessConnection);
        }
    }
}
