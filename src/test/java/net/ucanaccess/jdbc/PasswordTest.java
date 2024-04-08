package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

class PasswordTest extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testPassword(AccessVersion _accessVersion) throws Exception {

        assertThatThrownBy(() -> init(_accessVersion))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Authentication failed");

        try (UcanaccessConnection conn = new UcanaccessConnectionBuilder()
            .withDbPath(getAccessTempPath())
            .withPassword("ucanaccess")
            .build()) {
            assertNotNull(conn);
        }
    }
}
